package models

import (
	"fmt"
	"time"
)

// The Config struct defines all configuration parameters for the trading bot.
type Config struct {
	IsTestnet                bool      `json:"is_testnet"` // Use testnet?
	LiveAPIURL               string    `json:"live_api_url"`
	LiveWSURL                string    `json:"live_ws_url"`
	TestnetAPIURL            string    `json:"testnet_api_url"`
	TestnetWSURL             string    `json:"testnet_ws_url"`
	Symbol                   string    `json:"symbol"`                                // Trading pair, e.g. "BTCUSDT"
	GridSpacing              float64   `json:"grid_spacing"`                          // Grid spacing ratio.
	GridValue                float64   `json:"grid_value,omitempty"`                  // Trade value per grid (USDT)
	GridQuantity             float64   `json:"grid_quantity,omitempty"`               // Trade quantity per grid (base currency).
	MinNotionalValue         float64   `json:"min_notional_value"`                    // Exchange minimum notional order value (e.g. 5 USDT)
	InitialInvestment        float64   `json:"initial_investment"`                    // Initial investment (USDT), used for market buy
	Leverage                 int       `json:"leverage"`                              // Leverage multiplier
	MarginType               string    `json:"margin_type"`                           // Margin mode: CROSSED or ISOLATED
	PositionMode             string    `json:"position_mode"`                         // Position mode: "Oneway" or "Hedge"
	StopLossRate             float64   `json:"stop_loss_rate,omitempty"`              // New: Stop-loss rate
	HedgeMode                bool      `json:"hedge_mode"`                            // Enable hedge mode (dual positions)?
	GridCount                int       `json:"grid_count"`                            // Number of grids
	ActiveOrdersCount        int       `json:"active_orders_count"`                   // The number of orders hung on each side of the price
	ReturnRate               float64   `json:"return_rate"`                           // Expected mean reversion rate
	WalletExposureLimit      float64   `json:"wallet_exposure_limit"`                 // Wallet exposure limit
	LogConfig                LogConfig `json:"log"`                                   // Logging configuration
	RetryAttempts            int       `json:"retry_attempts"`                        // Number of retry attempts if order placement fails
	RetryInitialDelayMs      int       `json:"retry_initial_delay_ms"`                // Initial delay before retry (milliseconds)
	WebSocketPingIntervalSec int       `json:"websocket_ping_interval_sec,omitempty"` // Interval for sending WebSocket ping messages (seconds)
	WebSocketPongTimeoutSec  int       `json:"websocket_pong_timeout_sec,omitempty"`  // Timeout for WebSocket pong response (seconds)

	// 回测引擎特定配置
	TakerFeeRate          float64 `json:"taker_fee_rate"`          // 吃单手续费率
	MakerFeeRate          float64 `json:"maker_fee_rate"`          // 挂单手续费率
	SlippageRate          float64 `json:"slippage_rate"`           // 滑点率
	MaintenanceMarginRate float64 `json:"maintenance_margin_rate"` // 维持保证金率

	BaseURL   string `json:"base_url"`    // REST API base address, dynamically set by the program
	WSBaseURL string `json:"ws_base_url"` // WebSocket base address, dynamically set by the program
}

// LogConfig 定义了日志相关的配置
type LogConfig struct {
	Level      string `json:"level"`       // 日志级别, e.g., "debug", "info", "warn", "error"
	Output     string `json:"output"`      // 输出模式: "console", "file", "both"
	File       string `json:"file"`        // 日志文件路径
	MaxSize    int    `json:"max_size"`    // 单个日志文件的最大大小 (MB)
	MaxBackups int    `json:"max_backups"` // 保留的旧日志文件最大数量
	MaxAge     int    `json:"max_age"`     // 旧日志文件的最大保留天数
	Compress   bool   `json:"compress"`    // 是否压缩旧日志文件
}

// AccountInfo 定义了币安账户信息
type AccountInfo struct {
	TotalWalletBalance string `json:"totalWalletBalance"`
	AvailableBalance   string `json:"availableBalance"`
	Assets             []struct {
		Asset                  string `json:"asset"`
		WalletBalance          string `json:"walletBalance"`
		UnrealizedProfit       string `json:"unrealizedProfit"`
		MarginBalance          string `json:"marginBalance"`
		MaintMargin            string `json:"maintMargin"`
		InitialMargin          string `json:"initialMargin"`
		PositionInitialMargin  string `json:"positionInitialMargin"`
		OpenOrderInitialMargin string `json:"openOrderInitialMargin"`
		MaxWithdrawAmount      string `json:"maxWithdrawAmount"`
	} `json:"assets"`
}

// Position 定义了持仓信息
type Position struct {
	Symbol           string `json:"symbol"`
	PositionAmt      string `json:"positionAmt"`
	EntryPrice       string `json:"entryPrice"`
	MarkPrice        string `json:"markPrice"`
	UnrealizedProfit string `json:"unRealizedProfit"`
	LiquidationPrice string `json:"liquidationPrice"`
	Leverage         string `json:"leverage"`
	MaxNotionalValue string `json:"maxNotionalValue"`
	MarginType       string `json:"marginType"`
	IsolatedMargin   string `json:"isolatedMargin"`
	IsAutoAddMargin  string `json:"isAutoAddMargin"`
	PositionSide     string `json:"positionSide"`
	Notional         string `json:"notional"`
	IsolatedWallet   string `json:"isolatedWallet"`
	UpdateTime       int64  `json:"updateTime"`
}

// Order defines the order information
type Order struct {
	Symbol        string `json:"symbol"`
	OrderId       int64  `json:"orderId"`
	ClientOrderId string `json:"clientOrderId"`
	Price         string `json:"price"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	CumQuote      string `json:"cumQuote"`
	Status        string `json:"status"`
	TimeInForce   string `json:"timeInForce"`
	Type          string `json:"type"`
	Side          string `json:"side"`
	StopPrice     string `json:"stopPrice"`
	IcebergQty    string `json:"icebergQty"`
	Time          int64  `json:"time"`
	UpdateTime    int64  `json:"updateTime"`
	IsWorking     bool   `json:"isWorking"`
	WorkingType   string `json:"workingType"`
	OrigType      string `json:"origType"`
	PositionSide  string `json:"positionSide"`
	ActivatePrice string `json:"activatePrice"`
	PriceRate     string `json:"priceRate"`
	ReduceOnly    bool   `json:"reduceOnly"`
	ClosePosition bool   `json:"closePosition"`
	PriceProtect  bool   `json:"priceProtect"`
}

// GridLevel 代表网格中的一个价格档位
type GridLevel struct {
	Price           float64 `json:"price"`
	Quantity        float64 `json:"quantity"`
	Side            string  `json:"side"`
	IsActive        bool    `json:"is_active"`
	OrderID         int64   `json:"order_id"`
	GridID          int     `json:"grid_id"`                     // Record the theoretical grid ID associated with this order (index of conceptualGrid)
	PairID          int     `json:"pair_id"`                     // Used for matching buy and sell orders
	PairedSellPrice float64 `json:"paired_sell_price,omitempty"` // Used only in buy orders, records the corresponding sell price
}

// CompletedTrade records a finished trade (buy and sell)
type CompletedTrade struct {
	Symbol       string
	Quantity     float64
	EntryTime    time.Time
	ExitTime     time.Time
	HoldDuration time.Duration // Holding duration
	EntryPrice   float64
	ExitPrice    float64 // Entry price
	Profit       float64
	Fee          float64 // Transaction fee per trade
	Slippage     float64 // Slippage cost per trade
}

// BuyTrade records the details of a single buy-in for FIFO accounting.
type BuyTrade struct {
	Timestamp time.Time
	Quantity  float64
	Price     float64
}

// ExchangeInfo holds the full exchange information response
type ExchangeInfo struct {
	Symbols []SymbolInfo `json:"symbols"`
}

// SymbolInfo holds trading rules for a single symbol
type SymbolInfo struct {
	Symbol  string   `json:"symbol"`
	Filters []Filter `json:"filters"`
}

// Filter holds filter data, we are interested in PRICE_FILTER and LOT_SIZE
type Filter struct {
	FilterType  string `json:"filterType"`
	TickSize    string `json:"tickSize,omitempty"`    // For PRICE_FILTER
	StepSize    string `json:"stepSize,omitempty"`    // For LOT_SIZE
	MinQty      string `json:"minQty,omitempty"`      // For LOT_SIZE
	MaxQty      string `json:"maxQty,omitempty"`      // For LOT_SIZE
	MinNotional string `json:"minNotional,omitempty"` // For MIN_NOTIONAL
}

// A trade defines the information for a single transaction
type Trade struct {
	Symbol          string `json:"symbol"`
	ID              int64  `json:"id"`
	OrderID         int64  `json:"orderId"`
	Side            string `json:"side"`
	Price           string `json:"price"`
	Qty             string `json:"qty"`
	RealizedPnl     string `json:"realizedPnl"`
	MarginAsset     string `json:"marginAsset"`
	QuoteQty        string `json:"quoteQty"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
	Time            int64  `json:"time"`
	PositionSide    string `json:"positionSide"`
	Buyer           bool   `json:"buyer"`
	Maker           bool   `json:"maker"`
}

// TradeEvent defines a trade event from the WebSocket
type TradeEvent struct {
	EventType string `json:"e"` // Event type
	EventTime int64  `json:"E"` // Event time
	Symbol    string `json:"s"` // Symbol
	TradeID   int64  `json:"a"` // Aggregate trade ID
	Price     string `json:"p"` // Price
	Quantity  string `json:"q"` // Quantity
	FirstID   int64  `json:"f"` // First trade ID
	LastID    int64  `json:"l"` // Last trade ID
	TradeTime int64  `json:"T"` // Trade time
	IsMaker   bool   `json:"m"` // Is the buyer the market maker?
}

// UserDataEvent is a general event structure received from the user data stream
type UserDataEvent struct {
	EventType string `json:"e"` // Event type, e.g., "executionReport"
	EventTime int64  `json:"E"` // Event time
	// Depending on the type of event, different payloads can be included here.
	ExecutionReport ExecutionReport `json:"o"`
}

// ExecutionReport 包含了订单更新的详细信息
type ExecutionReport struct {
	Symbol        string `json:"s"`  // Symbol
	ClientOrderID string `json:"c"`  // Client Order ID
	Side          string `json:"S"`  // Side
	OrderType     string `json:"o"`  // Order Type
	TimeInForce   string `json:"f"`  // Time in Force
	OrigQty       string `json:"q"`  // Original Quantity
	Price         string `json:"p"`  // Price
	AvgPrice      string `json:"ap"` // Average Price
	StopPrice     string `json:"sp"` // Stop Price
	ExecType      string `json:"x"`  // Execution Type
	OrderStatus   string `json:"X"`  // Order Status
	OrderID       int64  `json:"i"`  // Order ID
	ExecutedQty   string `json:"l"`  // Last Executed Quantity
	CumQty        string `json:"z"`  // Cumulative Filled Quantity
	ExecutedPrice string `json:"L"`  // Last Executed Price
	CommissionAmt string `json:"n"`  // Commission Amount
	CommissionAs  string `json:"N"`  // Commission Asset
	TradeTime     int64  `json:"T"`  // Trade Time
	TradeID       int64  `json:"t"`  // Trade ID
}

// BotState defines the trading bot state that needs to be saved and loaded.
type BotState struct {
	GridLevels              []GridLevel `json:"grid_levels"`
	BasePositionEstablished bool        `json:"base_position_established"`
	ConceptualGrid          []float64   `json:"conceptual_grid"`
	EntryPrice              float64     `json:"entry_price"`
	ReversionPrice          float64     `json:"reversion_price"`
	IsReentering            bool        `json:"is_reentering"`
	CurrentPrice            float64     `json:"current_price"`
	CurrentTime             time.Time   `json:"current_time"`
}

// Balance 定义了账户中特定资产的余额信息
type Balance struct {
	Asset              string `json:"asset"`
	Balance            string `json:"balance"`
	CrossWalletBalance string `json:"crossWalletBalance"`
	AvailableBalance   string `json:"availableBalance"`
}

// Error defines the structure of the error information returned by the Binance API
type Error struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// Error 方法使得 BinanceError 实现了 error 接口
func (e *Error) Error() string {
	return fmt.Sprintf("API Error: code=%d, msg=%s", e.Code, e.Msg)
}

// GridState 定义了需要持久化保存的机器人状态
type GridState struct {
	GridLevels     []GridLevel `json:"grid_levels"`
	EntryPrice     float64     `json:"entry_price"`
	ReversionPrice float64     `json:"reversion_price"`
	ConceptualGrid []float64   `json:"conceptual_grid"`
}

// OrderUpdateEvent 是从用户数据流接收到的订单更新事件的完整结构
type OrderUpdateEvent struct {
	EventType       string          `json:"e"` // Event type, e.g., "ORDER_TRADE_UPDATE"
	EventTime       int64           `json:"E"` // Event time
	TransactionTime int64           `json:"T"` // Transaction time
	Order           OrderUpdateInfo `json:"o"` // Order information
}

// OrderUpdateInfo 包含了订单更新的具体信息
type OrderUpdateInfo struct {
	Symbol          string `json:"s"`  // Symbol
	ClientOrderID   string `json:"c"`  // Client Order ID
	Side            string `json:"S"`  // Side
	OrderType       string `json:"o"`  // Order Type
	TimeInForce     string `json:"f"`  // Time in Force
	OrigQty         string `json:"q"`  // Original Quantity
	Price           string `json:"p"`  // Price
	AvgPrice        string `json:"ap"` // Average Price
	StopPrice       string `json:"sp"` // Stop Price
	ExecutionType   string `json:"x"`  // Execution Type
	Status          string `json:"X"`  // Order Status
	OrderID         int64  `json:"i"`  // Order ID
	ExecutedQty     string `json:"l"`  // Last Executed Quantity
	CumQty          string `json:"z"`  // Cumulative Filled Quantity
	ExecutedPrice   string `json:"L"`  // Last Executed Price
	CommissionAmt   string `json:"n"`  // Commission Amount
	CommissionAsset string `json:"N"`  // Commission Asset, will be null if not traded
	TradeTime       int64  `json:"T"`  // Trade Time
	TradeID         int64  `json:"t"`  // Trade ID
	BidsNotional    string `json:"b"`  // Bids Notional
	AsksNotional    string `json:"a"`  // Asks Notional
	IsMaker         bool   `json:"m"`  // Is the trade a maker trade?
	IsReduceOnly    bool   `json:"R"`  // Is this a reduce only order?
	WorkingType     string `json:"wt"` // Stop Price Working Type
	OrigType        string `json:"ot"` // Original Order Type
	PositionSide    string `json:"ps"` // Position Side
	ClosePosition   bool   `json:"cp"` // If conditional order, is it close position?
	ActivationPrice string `json:"AP"` // Activation Price, only available for TRAILING_STOP_MARKET order
	CallbackRate    string `json:"cr"` // Callback Rate, only available for TRAILING_STOP_MARKET order
	RealizedProfit  string `json:"rp"` // Realized Profit of the trade
}

// AccountUpdateEvent represents the complete structure of the ACCOUNT_UPDATE WebSocket event.
type AccountUpdateEvent struct {
	EventType       string            `json:"e"` // Event Type
	EventTime       int64             `json:"E"` // Event Type
	TransactionTime int64             `json:"T"` // Matching engine transaction time.
	UpdateData      AccountUpdateData `json:"a"` // Detailed account update data.
}

// AccountUpdateData 包含账户更新中的余额和仓位信息。
type AccountUpdateData struct {
	Reason    string           `json:"m"` // The reason the event happened, for example, 'ORDER'
	Balances  []BalanceUpdate  `json:"B"` // Balance Update List
	Positions []PositionUpdate `json:"P"` // Position Update List
}

// BalanceUpdate represents a balance update for a single asset.
type BalanceUpdate struct {
	Asset              string `json:"a"`  // Asset name
	WalletBalance      string `json:"wb"` // Wallet balance
	CrossWalletBalance string `json:"cw"` // Cross margin wallet balance
	BalanceChange      string `json:"bc"` // Balance change (excluding PnL and fees)
}

// PositionUpdate represents an update for a single position.
type PositionUpdate struct {
	Symbol              string `json:"s"`   // Trading pair
	PositionAmount      string `json:"pa"`  // Position amount
	EntryPrice          string `json:"ep"`  // Average entry price
	AccumulatedRealized string `json:"cr"`  //  Accumulated realized profit and loss
	UnrealizedPnl       string `json:"up"`  // Unrealized profit and loss
	MarginType          string `json:"mt"`  // Margin type (cross or isolated)
	IsolatedWallet      string `json:"iw"`  // Isolated margin wallet balance
	PositionSide        string `json:"ps"`  // Position side (BOTH, LONG, SHORT)
	BreakEvenPrice      string `json:"bep"` // Break-even price
}
