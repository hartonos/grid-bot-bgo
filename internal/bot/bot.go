package bot

import (
	"binance-grid-bot-go/internal/exchange"
	"binance-grid-bot-go/internal/idgenerator"
	"binance-grid-bot-go/internal/logger"
	"binance-grid-bot-go/internal/models"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// EventType defines the type of a normalized event
type EventType int

const (
	OrderUpdateEvent EventType = iota
	// Add other event types here in the future, e.g., PriceTickEvent
)

// NormalizedEvent is a standardized internal representation of an event from any source
type NormalizedEvent struct {
	Type      EventType
	Timestamp time.Time
	Data      interface{} // Can be models.OrderUpdateEvent or other event data structs
}

// GridTradingBot is the core struct for the grid trading bot
type GridTradingBot struct {
	config                  *models.Config
	exchange                exchange.Exchange
	wsConn                  *websocket.Conn
	listenKey               string
	gridLevels              []models.GridLevel
	currentPrice            float64
	isRunning               bool
	IsBacktest              bool
	currentTime             time.Time
	basePositionEstablished bool
	entryPrice              float64
	reversionPrice          float64
	isReentering            bool
	reentrySignal           chan bool
	mutex                   sync.RWMutex
	stopChannel             chan bool
	eventChannel            chan NormalizedEvent // The central event queue
	symbolInfo              *models.SymbolInfo
	isHalted                bool
	safeModeReason          string
	idGenerator             *idgenerator.IDGenerator
}

// NewGridTradingBot creates a new instance of the grid trading bot
func NewGridTradingBot(config *models.Config, ex exchange.Exchange, isBacktest bool) *GridTradingBot {
	bot := &GridTradingBot{
		config:                  config,
		exchange:                ex,
		gridLevels:              make([]models.GridLevel, 0),
		isRunning:               false,
		IsBacktest:              isBacktest,
		basePositionEstablished: false,
		stopChannel:             make(chan bool),
		eventChannel:            make(chan NormalizedEvent, 1024), // Buffered channel
		reentrySignal:           make(chan bool, 1),
		isHalted:                false,
	}

	symbolInfo, err := ex.GetSymbolInfo(config.Symbol)
	if err != nil {
		logger.S().Fatalf("Could not get symbol info for %s: %v", config.Symbol, err)
	}
	bot.symbolInfo = symbolInfo
	logger.S().Infof("Successfully fetched and cached trading rules for %s.", config.Symbol)

	idGen, err := idgenerator.NewIDGenerator(0)
	if err != nil {
		logger.S().Fatalf("Could not create ID generator: %v", err)
	}
	bot.idGenerator = idGen

	return bot
}

// enterMarketAndSetupGrid implements the logic for entering the market and setting up the grid
func (b *GridTradingBot) enterMarketAndSetupGrid() error {
	logger.S().Info("--- Starting new trading cycle ---")

	currentPrice, err := b.exchange.GetPrice(b.config.Symbol)
	if err != nil {
		return fmt.Errorf("failed to get current price: %v", err)
	}

	b.mutex.Lock()
	b.currentPrice = currentPrice
	b.entryPrice = currentPrice
	b.reversionPrice = b.entryPrice * (1 + b.config.ReturnRate)
	b.gridLevels = make([]models.GridLevel, 0)
	b.isReentering = false
	// NEUTRAL GRID MODE: no initial market buy. SELL orders above entry will open/increase
	// a short position on fill; BUY orders below entry will open/increase a long position on fill.
	// This requires hedge_mode=false (one-way mode) on the Futures account.
	b.basePositionEstablished = true
	b.mutex.Unlock()

	logger.S().Infof("New cycle defined: Entry Price: %.4f, Reversion Price (cycle-restart trigger): %.4f", b.entryPrice, b.reversionPrice)
	logger.S().Info("Neutral grid mode: skipping initial market buy, placing capped-accelerating grid directly around entry price.")

	if err := b.setupInitialGrid(b.entryPrice); err != nil {
		return fmt.Errorf("initial grid setup failed: %v", err)
	}
	logger.S().Info("--- New cycle grid setup complete ---")

	return nil
}

// placeNewOrder is a helper function to place an order and return the result
func (b *GridTradingBot) placeNewOrder(side string, price float64, gridID int) (*models.GridLevel, error) {
	var tickSize string
	for _, f := range b.symbolInfo.Filters {
		if f.FilterType == "PRICE_FILTER" {
			tickSize = f.TickSize
		}
	}

	adjustedPrice := adjustValueToStep(price, tickSize)
	quantity, err := b.calculateQuantity(adjustedPrice)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate order quantity at price %.4f: %v", adjustedPrice, err)
	}

	signedQuantity := quantity
	if side == "SELL" {
		signedQuantity = -quantity
	}
	if !b.isWithinExposureLimit(signedQuantity) {
		return nil, fmt.Errorf("order blocked: wallet exposure limit would be exceeded")
	}

	clientOrderID, err := b.generateClientOrderID()
	if err != nil {
		return nil, fmt.Errorf("could not generate client order ID for grid order (GridID: %d): %v", gridID, err)
	}
	order, err := b.exchange.PlaceOrder(b.config.Symbol, side, "LIMIT", quantity, adjustedPrice, clientOrderID)
	if err != nil {
		return nil, fmt.Errorf("failed to place %s order at price %.4f: %v", side, adjustedPrice, err)
	}
	logger.S().Infof("Submitted %s order: ID %d, Price %.4f, Quantity %.5f, GridID: %d. Waiting for confirmation...", side, order.OrderId, adjustedPrice, quantity, gridID)

	if !b.IsBacktest {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		timeout := time.After(10 * time.Second)

		for {
			select {
			case <-ticker.C:
				status, err := b.exchange.GetOrderStatus(b.config.Symbol, order.OrderId)
				if err != nil {
					var binanceErr *models.Error
					if errors.As(err, &binanceErr) && binanceErr.Code == -2013 {
						logger.S().Warnf("Order %d not found yet, retrying...", order.OrderId)
						continue
					}
					return nil, fmt.Errorf("failed to get status for order %d: %v", order.OrderId, err)
				}

				switch status.Status {
				case "NEW", "PARTIALLY_FILLED", "FILLED":
					logger.S().Infof("Order %d confirmed by exchange with status: %s", order.OrderId, status.Status)
					goto confirmed
				case "CANCELED", "REJECTED", "EXPIRED":
					return nil, fmt.Errorf("order %d failed confirmation with final status: %s", order.OrderId, status.Status)
				}
			case <-timeout:
				return nil, fmt.Errorf("timeout waiting for order %d confirmation", order.OrderId)
			case <-b.stopChannel:
				return nil, fmt.Errorf("bot stopped, interrupting order %d confirmation", order.OrderId)
			}
		}
	}

confirmed:
	newGridLevel := &models.GridLevel{
		Price:    adjustedPrice,
		Quantity: quantity,
		Side:     side,
		IsActive: true,
		OrderID:  order.OrderId,
		GridID:   gridID,
	}
	logger.S().Infof("Successfully confirmed %s order: ID %d, Price %.4f, Quantity %.5f, GridID: %d", side, order.OrderId, adjustedPrice, quantity, gridID)
	return newGridLevel, nil
}

// calculateQuantity calculates and validates the order quantity based on configuration and exchange rules
func (b *GridTradingBot) calculateQuantity(price float64) (float64, error) {
	var quantity float64
	var minNotional, minQty, stepSize string

	for _, f := range b.symbolInfo.Filters {
		switch f.FilterType {
		case "MIN_NOTIONAL":
			minNotional = f.MinNotional
		case "LOT_SIZE":
			minQty = f.MinQty
			stepSize = f.StepSize
		}
	}

	minNotionalValue, _ := strconv.ParseFloat(minNotional, 64)
	minQtyValue, _ := strconv.ParseFloat(minQty, 64)

	if b.config.GridQuantity > 0 {
		quantity = b.config.GridQuantity
	} else if b.config.GridValue > 0 {
		quantity = b.config.GridValue / price
	} else {
		return 0, fmt.Errorf("neither grid_quantity nor grid_value is configured")
	}

	if price*quantity < minNotionalValue {
		quantity = (minNotionalValue / price) * 1.01
	}

	if quantity < minQtyValue {
		quantity = minQtyValue
	}

	adjustedQuantity := adjustValueToStep(quantity, stepSize)

	if adjustedQuantity < minQtyValue {
		step, _ := strconv.ParseFloat(stepSize, 64)
		if step > 0 {
			adjustedQuantity += step
			adjustedQuantity = adjustValueToStep(adjustedQuantity, stepSize)
		}
	}

	if price*adjustedQuantity < minNotionalValue {
		step, _ := strconv.ParseFloat(stepSize, 64)
		if step > 0 {
			adjustedQuantity += step
			adjustedQuantity = adjustValueToStep(adjustedQuantity, stepSize)
		}
	}

	return adjustedQuantity, nil
}

// connectWebSocket establishes a connection to the WebSocket
func (b *GridTradingBot) connectWebSocket() error {
	if b.IsBacktest {
		logger.S().Info("Backtest mode, skipping WebSocket connection.")
		return nil
	}

	listenKey, err := b.exchange.CreateListenKey()
	if err != nil {
		return fmt.Errorf("could not create listen key: %v", err)
	}
	b.listenKey = listenKey
	logger.S().Infof("Successfully obtained Listen Key: %s", b.listenKey)

	conn, err := b.exchange.ConnectWebSocket(b.listenKey)
	if err != nil {
		return fmt.Errorf("could not connect to WebSocket: %v", err)
	}
	b.wsConn = conn
	logger.S().Info("Successfully connected to user data stream WebSocket.")

	// Setup Pong Handler
	pongTimeout := time.Duration(b.config.WebSocketPongTimeoutSec) * time.Second
	if pongTimeout == 0 {
		pongTimeout = 75 * time.Second // Default value
	}
	if err = b.wsConn.SetReadDeadline(time.Now().Add(pongTimeout)); err != nil {
		return err
	}

	b.wsConn.SetPongHandler(func(string) error {
		if err := b.wsConn.SetReadDeadline(time.Now().Add(pongTimeout)); err != nil {
			return err
		}
		return nil
	})

	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := b.exchange.KeepAliveListenKey(b.listenKey); err != nil {
					logger.S().Warnf("Failed to keep listen key alive: %v", err)
				} else {
					logger.S().Info("Successfully kept listen key alive.")
				}
			case <-b.stopChannel:
				return
			}
		}
	}()

	return nil
}

// webSocketLoop listens for messages from the WebSocket
func (b *GridTradingBot) webSocketLoop() {
	if b.IsBacktest || b.wsConn == nil {
		return
	}

	readChannel := make(chan []byte)
	errChannel := make(chan error)

	go func() {
		for {
			_, message, err := b.wsConn.ReadMessage()
			if err != nil {
				errChannel <- err
				return
			}
			// Reset read deadline on successful message read
			pongTimeout := time.Duration(b.config.WebSocketPongTimeoutSec) * time.Second
			if pongTimeout == 0 {
				pongTimeout = 75 * time.Second // Default value
			}
			b.wsConn.SetReadDeadline(time.Now().Add(pongTimeout))
			readChannel <- message
		}
	}()

	// Ping Ticker
	pingInterval := time.Duration(b.config.WebSocketPingIntervalSec) * time.Second
	if pingInterval == 0 {
		pingInterval = 30 * time.Second // Default value
	}
	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()

	logger.S().Info("WebSocket message listener loop started.")

	for {
		select {
		case message := <-readChannel:
			b.handleWebSocketMessage(message)
		case <-pingTicker.C:
			if err := b.wsConn.WriteMessage(websocket.PingMessage, nil); err != nil {
				logger.S().Warnf("Failed to send WebSocket ping: %v", err)
			}
		case err := <-errChannel:
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				logger.S().Info("WebSocket connection closed normally.")
			} else {
				logger.S().Errorf("Error reading from WebSocket: %v. Starting reconnection process...", err)
				b.wsConn.Close() // Ensure the old connection is closed

				reconnectAttempts := 0
				for {
					reconnectAttempts++
					waitDuration := time.Duration(math.Min(float64(5*reconnectAttempts), 300)) * time.Second
					logger.S().Infof("Attempting to reconnect (attempt %d)... waiting for %v", reconnectAttempts, waitDuration)

					select {
					case <-time.After(waitDuration):
						if err := b.connectWebSocket(); err != nil {
							logger.S().Errorf("WebSocket reconnection attempt %d failed: %v", reconnectAttempts, err)
						} else {
							logger.S().Info("WebSocket reconnected successfully.")
							go b.webSocketLoop() // Restart the loop
							return               // Exit the old loop
						}
					case <-b.stopChannel:
						logger.S().Info("Stop signal received during reconnection, aborting.")
						return
					}
				}
			}
		case <-b.stopChannel:
			logger.S().Info("Stop signal received, closing WebSocket message loop.")
			b.wsConn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return
		}
	}
}

// Start starts the bot
func (b *GridTradingBot) Start() error {
	b.mutex.Lock()
	if b.isRunning {
		b.mutex.Unlock()
		return fmt.Errorf("bot is already running")
	}
	b.isRunning = true
	b.mutex.Unlock()

	logger.S().Info("Starting bot...")

	if !b.IsBacktest {
		if err := b.connectWebSocket(); err != nil {
			return fmt.Errorf("failed to connect to WebSocket on start: %v", err)
		}
		go b.webSocketLoop()
		go b.eventProcessor() // Start the core event processor
	}

	if err := b.enterMarketAndSetupGrid(); err != nil {
		b.enterSafeMode(fmt.Sprintf("failed to initialize grid and position: %v", err))
		return fmt.Errorf("failed to initialize grid and position: %v", err)
	}

	go b.strategyLoop()
	go b.monitorStatus()

	logger.S().Info("Bot started successfully.")
	return nil
}

// strategyLoop is the main strategy loop
func (b *GridTradingBot) strategyLoop() {
	for {
		select {
		case <-b.reentrySignal:
			logger.S().Info("Re-entry signal received, restarting trading cycle...")
			if err := b.enterMarketAndSetupGrid(); err != nil {
				logger.S().Errorf("Failed to re-enter market: %v", err)
				b.enterSafeMode(fmt.Sprintf("re-entry failed: %v", err))
			}
		case <-b.stopChannel:
			logger.S().Info("Strategy loop received stop signal, exiting.")
			return
		}
	}
}

// StartForBacktest starts the bot for backtesting
func (b *GridTradingBot) StartForBacktest() error {
	b.mutex.Lock()
	if b.isRunning {
		b.mutex.Unlock()
		return fmt.Errorf("bot is already running")
	}
	b.isRunning = true
	b.mutex.Unlock()

	logger.S().Info("Starting backtest bot...")
	if err := b.enterMarketAndSetupGrid(); err != nil {
		return fmt.Errorf("backtest initialization failed: %v", err)
	}
	logger.S().Info("Backtest bot initialized successfully.")
	return nil
}

// ProcessBacktestTick processes a single tick in backtest mode
func (b *GridTradingBot) ProcessBacktestTick() {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.isHalted {
		return
	}
}

// SetCurrentPrice sets the current price for backtesting
func (b *GridTradingBot) SetCurrentPrice(price float64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.currentPrice = price
}

// Stop stops the bot
func (b *GridTradingBot) Stop() {
	b.mutex.Lock()
	if !b.isRunning {
		b.mutex.Unlock()
		return
	}
	b.isRunning = false
	close(b.stopChannel)
	b.mutex.Unlock()

	logger.S().Info("Stopping bot...")
	b.cancelAllActiveOrders()
	if b.wsConn != nil {
		b.wsConn.Close()
	}
	logger.S().Info("Bot stopped.")
}

// cancelAllActiveOrders cancels all active orders
func (b *GridTradingBot) cancelAllActiveOrders() {
	logger.S().Info("Cancelling all active orders...")
	if err := b.exchange.CancelAllOpenOrders(b.config.Symbol); err != nil {
		logger.S().Warnf("Failed to cancel all orders: %v", err)
	} else {
		logger.S().Info("Successfully sent request to cancel all orders.")
	}
}

// monitorStatus prints the bot's status periodically
func (b *GridTradingBot) monitorStatus() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			//  b.printStatus()
		case <-b.stopChannel:
			logger.S().Info("Status monitor received stop signal, exiting.")
			return
		}
	}
}

// printStatus prints the current status of the bot
func (b *GridTradingBot) printStatus() {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if b.isHalted {
		logger.S().Warnf("Bot is halted. Reason: %s", b.safeModeReason)
		return
	}

	logger.S().Info("--- Bot Status Report ---")
	logger.S().Infof("Running: %v", b.isRunning)
	logger.S().Infof("Current Price: %.4f", b.currentPrice)
	logger.S().Infof("Active Orders: %d", len(b.gridLevels))
	for _, level := range b.gridLevels {
		logger.S().Infof("  - %s Order: ID %d, Price %.4f, Quantity %.5f", level.Side, level.OrderID, level.Price, level.Quantity)
	}
	logger.S().Info("-------------------------")
}

// adjustValueToStep adjusts a value to the given step size
func adjustValueToStep(value float64, step string) float64 {
	if step == "" || step == "0" {
		return value
	}
	stepFloat, err := strconv.ParseFloat(step, 64)
	if err != nil || stepFloat == 0 {
		return value
	}
	multiplier := 1.0 / stepFloat
	return math.Floor(value*multiplier) / multiplier
}

// spacingForLevel returns the % distance (as a fraction, e.g. 0.05 = 5%) between
// level (rank-1) and level (rank) in the capped accelerating grid schedule.
// rank=1 -> GridSpacingStart, then grows by GridSpacingIncrement per level,
// capped at GridSpacingCap.
func spacingForLevel(rank int, start, increment, cap float64) float64 {
	spacing := start + float64(rank-1)*increment
	if spacing > cap {
		spacing = cap
	}
	return spacing
}

// computeGridLevels generates `count` SELL price levels above pivotPrice and `count`
// BUY price levels below pivotPrice, using the capped accelerating spacing schedule.
// Each level compounds from the previous level's price (not from the pivot directly).
// sellLevels[0] is the closest level above pivot (rank 1), sellLevels[len-1] is the
// farthest (highest rank). buyLevels is symmetric on the downside.
func (b *GridTradingBot) computeGridLevels(pivotPrice float64, count int, tickSize string) (sellLevels []float64, buyLevels []float64) {
	start := b.config.GridSpacingStart
	increment := b.config.GridSpacingIncrement
	cap := b.config.GridSpacingCap

	sellPrice := pivotPrice
	for rank := 1; rank <= count; rank++ {
		spacing := spacingForLevel(rank, start, increment, cap)
		sellPrice = sellPrice * (1 + spacing)
		sellLevels = append(sellLevels, adjustValueToStep(sellPrice, tickSize))
	}

	buyPrice := pivotPrice
	for rank := 1; rank <= count; rank++ {
		spacing := spacingForLevel(rank, start, increment, cap)
		buyPrice = buyPrice * (1 - spacing)
		buyLevels = append(buyLevels, adjustValueToStep(buyPrice, tickSize))
	}

	return sellLevels, buyLevels
}

// generateClientOrderID generates a new client order ID
func (b *GridTradingBot) generateClientOrderID() (string, error) {
	id, err := b.idGenerator.Generate()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("x-grid-%s", id), nil
}

// isWithinExposureLimit checks if adding a trade would exceed the wallet exposure limit
func (b *GridTradingBot) isWithinExposureLimit(quantityToAdd float64) bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if b.config.WalletExposureLimit <= 0 {
		return true
	}

	positions, err := b.exchange.GetPositions(b.config.Symbol)
	if err != nil {
		logger.S().Warnf("Could not get positions to check exposure limit: %v", err)
		return false
	}

	var currentPositionSize float64
	if len(positions) > 0 {
		currentPositionSize, _ = strconv.ParseFloat(positions[0].PositionAmt, 64)
	}

	_, accountEquity, err := b.exchange.GetAccountState(b.config.Symbol)
	if err != nil {
		logger.S().Warnf("Could not get account state to check exposure limit: %v", err)
		return false
	}

	if accountEquity <= 0 {
		return false
	}

	futurePositionValue := (currentPositionSize + quantityToAdd) * b.currentPrice
	expectedExposure := math.Abs(futurePositionValue) / accountEquity

	if expectedExposure > b.config.WalletExposureLimit {
		logger.S().Warnf(
			"Wallet exposure check failed: Expected exposure %.2f%% would exceed limit of %.2f%%.",
			expectedExposure*100, b.config.WalletExposureLimit*100,
		)
		return false
	}
	return true
}

// IsHalted returns whether the bot is halted
func (b *GridTradingBot) IsHalted() bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.isHalted
}

// handleWebSocketMessage parses and handles messages from the WebSocket
func (b *GridTradingBot) handleWebSocketMessage(message []byte) {
	var data map[string]interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		logger.S().Warnf("Could not unmarshal WebSocket message into map: %v, Raw: %s", err, string(message))
		return
	}

	eventType, ok := data["e"].(string)
	if !ok {
		logger.S().Debugf("Received event with non-string or missing event type: %s", string(message))
		return
	}

	switch eventType {
	case "ORDER_TRADE_UPDATE":
		var orderUpdateEvent models.OrderUpdateEvent
		if err := json.Unmarshal(message, &orderUpdateEvent); err != nil {
			logger.S().Warnf("Could not unmarshal order trade update event: %v, Raw: %s", err, string(message))
			return
		}
		// Instead of handling it directly, push it to the event channel
		b.eventChannel <- NormalizedEvent{
			Type:      OrderUpdateEvent,
			Timestamp: time.Now(),
			Data:      orderUpdateEvent,
		}
	case "ACCOUNT_UPDATE":
		// Placeholder for handling account updates if needed in the future.
		// To implement, create a specific struct for ACCOUNT_UPDATE and unmarshal here.
	case "TRADE_LITE":
		// This is a public trade event, not specific to our orders. We can safely ignore it.
	default:
		// Optionally log unknown event types for future analysis, but avoid spamming.
		// logger.S().Debugf("Received unhandled event type '%s'", eventType)
	}
}

// handleOrderUpdate is now called sequentially by the event processor.
// It no longer needs to manage its own concurrency with goroutines or the isRebuilding flag.
func (b *GridTradingBot) handleOrderUpdate(event models.OrderUpdateEvent) {
	if event.Order.ExecutionType != "TRADE" || event.Order.Status != "FILLED" {
		return
	}

	o := event.Order
	logger.S().Infof("--- Processing Order Fill Event ---")
	logger.S().Infof("Order ID: %d, Symbol: %s, Side: %s, Price: %s, Quantity: %s",
		o.OrderID, o.Symbol, o.Side, o.Price, o.OrigQty)

	var triggeredGrid *models.GridLevel
	b.mutex.RLock()
	for i := range b.gridLevels {
		if b.gridLevels[i].OrderID == o.OrderID {
			// Create a copy to avoid holding the lock during rebuild
			gridCopy := b.gridLevels[i]
			triggeredGrid = &gridCopy
			break
		}
	}
	b.mutex.RUnlock()

	if triggeredGrid == nil {
		logger.S().Warnf("Received fill update for order %d, but no matching active grid level found. This could be a manually placed order or from a previous session.", o.OrderID)
		return
	}

	logger.S().Infof("Matched active grid order: rank %d, side %s, Price %.4f", triggeredGrid.GridID, triggeredGrid.Side, triggeredGrid.Price)
	filledPrice, _ := strconv.ParseFloat(o.Price, 64)

	b.mutex.RLock()
	reversionPrice := b.reversionPrice
	b.mutex.RUnlock()

	// Cycle completion check: price has reached or exceeded the reversion target,
	// meaning the grid has extended far enough that we restart the whole cycle
	// (re-evaluate entry price from the live market) instead of continuing to extend.
	if filledPrice >= reversionPrice {
		logger.S().Infof("Price %.4f has reached or exceeded reversion price %.4f. Triggering cycle restart.", filledPrice, reversionPrice)
		select {
		case b.reentrySignal <- true:
		default:
			logger.S().Warn("Re-entry signal channel is full. Cycle restart may be delayed.")
		}
		return
	}

	if err := b.rebuildGrid(*triggeredGrid, filledPrice); err != nil {
		// If rebuild fails, we must enter safe mode to prevent further trading with a broken state.
		b.enterSafeMode(fmt.Sprintf("Failed to rebuild grid after fill of order %d: %v", o.OrderID, err))
	}
}

// rebuildGrid implements the hybrid capped-accelerating rebuild rule:
//   - The side that just filled (triggeredGrid.Side) is "extended": the filled level is
//     dropped, surviving unfilled levels on that side are left untouched, and exactly one
//     new level is added at the far end, continuing the spacing schedule from the farthest
//     surviving level (or from the fill price if none survive).
//   - The opposite side is fully rebuilt: all its existing orders are cancelled and replaced
//     with a fresh set of `active_orders_count` levels computed from the fill price, so it
//     always stays anchored close to the current price with no gap.
func (b *GridTradingBot) rebuildGrid(triggeredGrid models.GridLevel, filledPrice float64) error {
	triggeredSide := triggeredGrid.Side
	oppositeSide := "BUY"
	if triggeredSide == "BUY" {
		oppositeSide = "SELL"
	}
	logger.S().Infof("--- Hybrid grid rebuild: %s side filled at %.4f (rank %d). Extending %s, refreshing %s. ---",
		triggeredSide, filledPrice, triggeredGrid.GridID, triggeredSide, oppositeSide)

	b.mutex.RLock()
	var sameSideRemaining []models.GridLevel
	var oppositeSideOrders []models.GridLevel
	for _, gl := range b.gridLevels {
		if gl.OrderID == triggeredGrid.OrderID {
			continue // this one just filled, drop it
		}
		if gl.Side == triggeredSide {
			sameSideRemaining = append(sameSideRemaining, gl)
		} else {
			oppositeSideOrders = append(oppositeSideOrders, gl)
		}
	}
	activeOrdersCount := b.config.ActiveOrdersCount
	b.mutex.RUnlock()

	// Step 1: cancel all opposite-side orders (they're being fully refreshed).
	for _, ol := range oppositeSideOrders {
		if err := b.exchange.CancelOrder(b.config.Symbol, ol.OrderID); err != nil {
			reason := fmt.Sprintf("failed to cancel opposite-side order %d during rebuild: %v", ol.OrderID, err)
			b.enterSafeMode(reason)
			return errors.New(reason)
		}
	}

	var tickSize string
	for _, f := range b.symbolInfo.Filters {
		if f.FilterType == "PRICE_FILTER" {
			tickSize = f.TickSize
		}
	}

	// Step 2: extend the triggered side with exactly one new far level.
	var extendedOrder *models.GridLevel
	anchorPrice := filledPrice
	anchorRank := triggeredGrid.GridID
	for _, gl := range sameSideRemaining {
		if gl.GridID > anchorRank {
			anchorRank = gl.GridID
			anchorPrice = gl.Price
		}
	}
	newRank := anchorRank + 1
	spacing := spacingForLevel(newRank, b.config.GridSpacingStart, b.config.GridSpacingIncrement, b.config.GridSpacingCap)
	var newPrice float64
	if triggeredSide == "SELL" {
		newPrice = anchorPrice * (1 + spacing)
	} else {
		newPrice = anchorPrice * (1 - spacing)
	}
	newPrice = adjustValueToStep(newPrice, tickSize)

	order, err := b.placeNewOrder(triggeredSide, newPrice, newRank)
	if err != nil {
		reason := fmt.Sprintf("failed to extend %s side after fill: %v", triggeredSide, err)
		b.enterSafeMode(reason)
		return errors.New(reason)
	}
	extendedOrder = order

	// Step 3: place a fresh set of opposite-side orders anchored at the fill price.
	sellLevels, buyLevels := b.computeGridLevels(filledPrice, activeOrdersCount, tickSize)
	freshLevels := buyLevels
	if oppositeSide == "SELL" {
		freshLevels = sellLevels
	}

	var newOppositeOrders []models.GridLevel
	for i, price := range freshLevels {
		rank := i + 1
		o, err := b.placeNewOrder(oppositeSide, price, rank)
		if err != nil {
			reason := fmt.Sprintf("failed to place fresh %s order (rank %d) during rebuild: %v", oppositeSide, rank, err)
			b.enterSafeMode(reason)
			return errors.New(reason)
		}
		newOppositeOrders = append(newOppositeOrders, *o)
	}

	// Step 4: commit the new state.
	newGridLevels := make([]models.GridLevel, 0, len(sameSideRemaining)+1+len(newOppositeOrders))
	newGridLevels = append(newGridLevels, sameSideRemaining...)
	newGridLevels = append(newGridLevels, *extendedOrder)
	newGridLevels = append(newGridLevels, newOppositeOrders...)

	b.mutex.Lock()
	b.gridLevels = newGridLevels
	b.mutex.Unlock()

	logger.S().Infof("--- Hybrid rebuild complete: %s extended (new rank %d @ %.4f), %s refreshed (%d orders). %d orders total. ---",
		triggeredSide, newRank, newPrice, oppositeSide, len(newOppositeOrders), len(newGridLevels))
	return nil
}

// setupInitialGrid places SELL/BUY orders around a center price using the capped
// accelerating grid spacing schedule. GridID here means "rank" within its side
// (1 = closest to pivot, 2 = next, ...), not an index into any shared array.
func (b *GridTradingBot) setupInitialGrid(centerPrice float64) error {
	logger.S().Infof("--- Setting up initial grid, center price: %.4f ---", centerPrice)

	b.mutex.Lock()
	b.gridLevels = make([]models.GridLevel, 0)
	b.mutex.Unlock()

	var tickSize string
	for _, f := range b.symbolInfo.Filters {
		if f.FilterType == "PRICE_FILTER" {
			tickSize = f.TickSize
		}
	}

	activeOrdersCount := b.config.ActiveOrdersCount
	sellPrices, buyPrices := b.computeGridLevels(centerPrice, activeOrdersCount, tickSize)

	var wg sync.WaitGroup
	newOrdersChan := make(chan *models.GridLevel, activeOrdersCount*2)
	errChan := make(chan error, activeOrdersCount*2)

	for i, price := range sellPrices {
		rank := i + 1
		wg.Add(1)
		go func(price float64, rank int) {
			defer wg.Done()
			if order, err := b.placeNewOrder("SELL", price, rank); err != nil {
				errChan <- fmt.Errorf("failed to place sell order (rank %d): %v", rank, err)
			} else {
				newOrdersChan <- order
			}
		}(price, rank)
	}

	for i, price := range buyPrices {
		rank := i + 1
		wg.Add(1)
		go func(price float64, rank int) {
			defer wg.Done()
			if order, err := b.placeNewOrder("BUY", price, rank); err != nil {
				errChan <- fmt.Errorf("failed to place buy order (rank %d): %v", rank, err)
			} else {
				newOrdersChan <- order
			}
		}(price, rank)
	}

	wg.Wait()
	close(newOrdersChan)
	close(errChan)

	var finalError error
	for err := range errChan {
		if finalError == nil {
			finalError = err
		}
		logger.S().Error(err)
	}

	b.mutex.Lock()
	for order := range newOrdersChan {
		b.gridLevels = append(b.gridLevels, *order)
	}
	b.mutex.Unlock()

	if finalError != nil {
		reason := fmt.Sprintf("one or more orders failed during initial grid setup: %v", finalError)
		b.enterSafeMode(reason)
		return errors.New(reason)
	}

	logger.S().Infof("--- Initial grid setup complete, %d new orders placed ---", len(b.gridLevels))
	return nil
}

// enterSafeMode puts the bot into a safe mode where it stops trading
func (b *GridTradingBot) enterSafeMode(reason string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.isHalted {
		return
	}
	b.isHalted = true
	b.safeModeReason = reason
	logger.S().Errorf("--- Entering Safe Mode ---")
	logger.S().Errorf("Reason: %s", reason)
	logger.S().Errorf("Bot has stopped all trading activity. Manual intervention required.")
	go b.cancelAllActiveOrders()
}

// eventProcessor is the heart of the bot, processing all events sequentially from a single channel.
// This architectural choice eliminates race conditions for state modifications.
func (b *GridTradingBot) eventProcessor() {
	logger.S().Info("Core event processor started.")
	for {
		select {
		case event := <-b.eventChannel:
			b.processSingleEvent(event)
		case <-b.stopChannel:
			logger.S().Info("Core event processor stopped.")
			return
		}
	}
}

// processSingleEvent handles a single normalized event.
// All state-modifying logic should be called from here.
func (b *GridTradingBot) processSingleEvent(event NormalizedEvent) {
	switch event.Type {
	case OrderUpdateEvent:
		if orderUpdate, ok := event.Data.(models.OrderUpdateEvent); ok {
			b.handleOrderUpdate(orderUpdate)
		} else {
			logger.S().Warnf("Received OrderUpdateEvent with unexpected data type: %T", event.Data)
		}
		// Future event types can be handled here
		// case PriceTickEvent:
		// ...
	}
}
