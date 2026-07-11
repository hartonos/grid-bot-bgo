package exchange

import (
	"binance-grid-bot-go/internal/models"
	"time"

	"github.com/gorilla/websocket"
)

// NewOrderRequest defines all the parameters required to create a new order.
// This is a standardized structure used to pass information between the bot and the exchange implementation.
type NewOrderRequest struct {
	Symbol   string
	Side     string
	Type     string
	Price    float64
	Quantity float64
}

// Exchange defines all the common methods that every exchange implementation must provide.
// This allows the trading bot to easily switch between live trading and backtesting.
type Exchange interface {
	GetPrice(symbol string) (float64, error)
	GetPositions(symbol string) ([]models.Position, error)
	PlaceOrder(symbol, side, orderType string, quantity, price float64, clientOrderID string) (*models.Order, error)
	CancelOrder(symbol string, orderID int64) error
	SetLeverage(symbol string, leverage int) error
	SetPositionMode(isHedgeMode bool) error
	GetPositionMode() (bool, error)
	SetMarginType(symbol string, marginType string) error
	GetMarginType(symbol string) (string, error)
	GetAccountInfo() (*models.AccountInfo, error)
	CancelAllOpenOrders(symbol string) error
	GetOrderStatus(symbol string, orderID int64) (*models.Order, error)
	GetCurrentTime() time.Time
	// GetAccountState retrieves the account state, including total position value and total account equity
	GetAccountState(symbol string) (positionValue float64, accountEquity float64, err error)
	GetSymbolInfo(symbol string) (*models.SymbolInfo, error)
	GetOpenOrders(symbol string) ([]models.Order, error) // New: Get all pending orders
	GetServerTime() (int64, error)                       // New: Get server time
	GetLastTrade(symbol string, orderID int64) (*models.Trade, error)
	GetMaxWalletExposure() float64
	CreateListenKey() (string, error)
	KeepAliveListenKey(listenKey string) error
	GetBalance() (float64, error)
	ConnectWebSocket(listenKey string) (*websocket.Conn, error)
}
