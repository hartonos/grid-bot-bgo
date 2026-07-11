package bot

import (
	"binance-grid-bot-go/internal/exchange"
	"binance-grid-bot-go/internal/idgenerator"
	"binance-grid-bot-go/internal/logger"
	"binance-grid-bot-go/internal/models"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
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
	basePositionEstablished bool // Kept for compatibility, but always true in neutral mode Mistralai
	conceptualGrid          []float64
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

// establishBasePositionAndWait tries to establish the initial base position and waits for it to be filled
// establishBasePositionAndWait is now unused in neutral mode. Mistralai
// enterMarketAndSetupGrid sets up a neutral grid (no initial position)
func (b *GridTradingBot) enterMarketAndSetupGrid() error {
	logger.S().Info("--- Starting new trading cycle (Neutral Grid) ---")

	currentPrice, err := b.exchange.GetPrice(b.config.Symbol)
	if err != nil {
		return fmt.Errorf("failed to get current price: %v", err)
	}

	b.mutex.Lock()
	b.currentPrice = currentPrice
	b.entryPrice = currentPrice
	b.reversionPrice = b.entryPrice * (1 + b.config.ReturnRate)
	b.gridLevels = make([]models.GridLevel, 0)
	b.conceptualGrid = make([]float64, 0)
	b.basePositionEstablished = true // Always true for neutral grid
	b.mutex.Unlock()

	logger.S().Infof("New cycle defined: Entry Price: %.4f, Reversion Price (Grid Top): %.4f", b.entryPrice, b.reversionPrice)

	// Generate conceptual grid (same as before)
	b.mutex.Lock()
	var tickSize string
	for _, f := range b.symbolInfo.Filters {
		if f.FilterType == "PRICE_FILTER" {
			tickSize = f.TickSize
		}
	}

	price := b.reversionPrice
	for price > (b.entryPrice * 0.5) {
		adjustedPrice := adjustValueToStep(price, tickSize)
		if len(b.conceptualGrid) == 0 || b.conceptualGrid[len(b.conceptualGrid)-1] != adjustedPrice {
			b.conceptualGrid = append(b.conceptualGrid, adjustedPrice)
		}
		price *= 1 - b.config.GridSpacing
	}
	b.mutex.Unlock()

	if len(b.conceptualGrid) == 0 {
		logger.S().Warn("Conceptual grid is empty, likely due to misconfiguration of return rate or grid spacing. Skipping grid setup.")
		return nil
	}
	logger.S().Infof("Successfully generated conceptual grid with %d levels.", len(b.conceptualGrid))

	// Directly set up neutral grid (no initial market order)
	err = b.setupInitialGrid(b.entryPrice)
	if err != nil {
		return fmt.Errorf("neutral grid setup failed: %v", err)
	}

	logger.S().Info("--- Neutral grid setup complete ---")
	return nil
}

// setupInitialGrid places buy and sell orders around the center price (neutral grid)
func (b *GridTradingBot) setupInitialGrid(centerPrice float64) error {
	logger.S().Infof("--- Setting up neutral grid, center price: %.4f ---", centerPrice)

	b.mutex.Lock()
	b.gridLevels = make([]models.GridLevel, 0)
	b.mutex.Unlock()

	b.mutex.RLock()
	pivotGridID := -1
	minDiff := math.MaxFloat64
	for i, p := range b.conceptualGrid {
		if math.Abs(p-centerPrice) < minDiff {
			minDiff = math.Abs(p - centerPrice)
			pivotGridID = i
		}
	}
	conceptualGridCopy := make([]float64, len(b.conceptualGrid))
	copy(conceptualGridCopy, b.conceptualGrid)
	activeOrdersCount := b.config.ActiveOrdersCount
	b.mutex.RUnlock()

	if pivotGridID == -1 {
		reason := fmt.Sprintf("could not find pivot grid ID for center price %.4f", centerPrice)
		b.enterSafeMode(reason)
		return errors.New(reason)
	}
	logger.S().Infof("Found closest pivot grid ID: %d (Price: %.4f)", pivotGridID, conceptualGridCopy[pivotGridID])

	var wg sync.WaitGroup
	newOrdersChan := make(chan *models.GridLevel, activeOrdersCount*2)
	errChan := make(chan error, activeOrdersCount*2)

	// Place sell orders above pivot
	for i := 1; i <= activeOrdersCount; i++ {
		index := pivotGridID - i
		if index >= 0 && index < len(conceptualGridCopy) {
			wg.Add(1)
			go func(price float64, gridID int) {
				defer wg.Done()
				if order, err := b.placeNewOrder("SELL", price, gridID); err != nil {
					errChan <- fmt.Errorf("failed to place sell order (GridID %d): %v", gridID, err)
				} else {
					newOrdersChan <- order
				}
			}(conceptualGridCopy[index], index)
		}
	}

	// Place buy orders below pivot
	for i := 1; i <= activeOrdersCount; i++ {
		index := pivotGridID + i
		if index >= 0 && index < len(conceptualGridCopy) {
			wg.Add(1)
			go func(price float64, gridID int) {
				defer wg.Done()
				if order, err := b.placeNewOrder("BUY", price, gridID); err != nil {
					errChan <- fmt.Errorf("failed to place buy order (GridID %d): %v", gridID, err)
				} else {
					newOrdersChan <- order
				}
			}(conceptualGridCopy[index], index)
		}
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
		reason := fmt.Sprintf("one or more orders failed during neutral grid setup: %v", finalError)
		b.enterSafeMode(reason)
		return errors.New(reason)
	}

	logger.S().Infof("--- Neutral grid setup complete, %d new orders placed ---", len(b.gridLevels))
	return nil
}

// rebuildGrid recenters the grid around the filled price (neutral grid)
func (b *GridTradingBot) rebuildGrid(pivotGridID int) error {
	logger.S().Infof("--- Starting grid rebuild, pivot GridID: %d ---", pivotGridID)

	logger.S().Info("Step 1/3: Cancelling all existing orders...")
	if err := b.exchange.CancelAllOpenOrders(b.config.Symbol); err != nil {
		reason := fmt.Sprintf("failed to cancel orders during grid rebuild: %v", err)
		b.enterSafeMode(reason)
		return errors.New(reason)
	}

	logger.S().Info("Step 2/3: Waiting for exchange to confirm all orders are cancelled...")
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			orders, err := b.exchange.GetOpenOrders(b.config.Symbol)
			if err != nil {
				reason := fmt.Sprintf("failed to get open orders while confirming cancellation: %v", err)
				b.enterSafeMode(reason)
				return errors.New(reason)
			}
			if len(orders) == 0 {
				logger.S().Info("All orders confirmed cancelled.")
				goto allCancelled
			}
			logger.S().Infof("Still waiting for %d orders to be cancelled...", len(orders))
		case <-timeout:
			reason := "timeout waiting for order cancellation confirmation"
			b.enterSafeMode(reason)
			return errors.New(reason)
		case <-b.stopChannel:
			return errors.New("bot stopped, interrupting grid rebuild")
		}
	}

allCancelled:
	logger.S().Info("Step 3/3: Placing new grid orders...")
	b.mutex.Lock()
	b.gridLevels = make([]models.GridLevel, 0)
	b.mutex.Unlock()

	b.mutex.RLock()
	conceptualGridCopy := make([]float64, len(b.conceptualGrid))
	copy(conceptualGridCopy, b.conceptualGrid)
	activeOrdersCount := b.config.ActiveOrdersCount
	b.mutex.RUnlock()

	if pivotGridID < 0 || pivotGridID >= len(conceptualGridCopy) {
		reason := fmt.Sprintf("invalid pivotGridID: %d", pivotGridID)
		b.enterSafeMode(reason)
		return errors.New(reason)
	}
	logger.S().Infof("Using pivot GridID: %d (Price: %.4f)", pivotGridID, conceptualGridCopy[pivotGridID])

	var wg sync.WaitGroup
	newOrdersChan := make(chan *models.GridLevel, activeOrdersCount*2)
	errChan := make(chan error, activeOrdersCount*2)

	// Place sell orders above pivot
	for i := 1; i <= activeOrdersCount; i++ {
		sellIndex := pivotGridID - i
		if sellIndex < 0 {
			break
		}
		wg.Add(1)
		go func(price float64, gridID int) {
			defer wg.Done()
			if order, err := b.placeNewOrder("SELL", price, gridID); err != nil {
				errChan <- fmt.Errorf("failed to place sell order (GridID %d): %v", gridID, err)
			} else {
				newOrdersChan <- order
			}
		}(conceptualGridCopy[sellIndex], sellIndex)
	}

	// Place buy orders below pivot
	for i := 1; i <= activeOrdersCount; i++ {
		buyIndex := pivotGridID + i
		if buyIndex >= len(conceptualGridCopy) {
			break
		}
		wg.Add(1)
		go func(price float64, gridID int) {
			defer wg.Done()
			if order, err := b.placeNewOrder("BUY", price, gridID); err != nil {
				errChan <- fmt.Errorf("failed to place buy order (GridID %d): %v", gridID, err)
			} else {
				newOrdersChan <- order
			}
		}(conceptualGridCopy[buyIndex], buyIndex)
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

	// Collect new orders
	newGridLevels := make([]models.GridLevel, 0, activeOrdersCount*2)
	for order := range newOrdersChan {
		newGridLevels = append(newGridLevels, *order)
	}

	b.mutex.Lock()
	b.gridLevels = newGridLevels
	b.mutex.Unlock()

	if finalError != nil {
		reason := fmt.Sprintf("one or more orders failed during grid rebuild: %v", finalError)
		b.enterSafeMode(reason)
		return errors.New(reason)
	}

	logger.S().Infof("--- Grid rebuild complete, %d new orders placed ---", len(b.gridLevels))
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

	if side == "BUY" && !b.isWithinExposureLimit(quantity) {
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
					if strings.Contains(err.Error(), "order does not exist") {
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

// SaveState saves the bot's state to a file
func (b *GridTradingBot) SaveState(path string) error {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	state := models.GridState{
		GridLevels:     b.gridLevels,
		EntryPrice:     b.entryPrice,
		ReversionPrice: b.reversionPrice,
		ConceptualGrid: b.conceptualGrid,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("could not serialize bot state: %v", err)
	}

	return ioutil.WriteFile(path, data, 0644)
}

// LoadState loads the bot's state from a file
func (b *GridTradingBot) LoadState(path string) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			logger.S().Info("State file not found, starting from initial state.")
			return nil
		}
		return fmt.Errorf("could not read state file: %v", err)
	}

	var state models.GridState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("could not deserialize bot state: %v", err)
	}

	b.mutex.Lock()
	b.gridLevels = state.GridLevels
	b.entryPrice = state.EntryPrice
	b.reversionPrice = state.ReversionPrice
	b.conceptualGrid = state.ConceptualGrid
	b.basePositionEstablished = true
	b.mutex.Unlock()

	logger.S().Infof("Successfully loaded bot state from %s.", path)
	return b.syncWithExchange()
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
	expectedExposure := futurePositionValue / accountEquity

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

// syncWithExchange synchronizes the local state with the exchange's state
// HAPUS
	
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
