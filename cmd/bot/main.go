package main

import (
	"binance-grid-bot-go/internal/bot"
	"binance-grid-bot-go/internal/config"
	"binance-grid-bot-go/internal/downloader"
	"binance-grid-bot-go/internal/exchange"
	"binance-grid-bot-go/internal/logger" // Add a new logger package
	"binance-grid-bot-go/internal/models"
	"binance-grid-bot-go/internal/reporter"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

// extractSymbolFromPath 从数据文件路径中提取交易对名称
// 例如: "data/BNBUSDT-2025-03-15-2025-06-15.csv" -> "BNBUSDT"
func extractSymbolFromPath(path string) string {
	// 移除目录和 .csv 后缀
	name := strings.TrimSuffix(path, ".csv")
	parts := strings.Split(name, "/")
	fileName := parts[len(parts)-1]

	// 按 "-" 分割并取第一部分
	symbolParts := strings.Split(fileName, "-")
	if len(symbolParts) > 0 {
		return symbolParts[0]
	}
	return ""
}

func main() {
	// --- 命令行参数定义 ---
	configPath := flag.String("config", "config.json", "path to the config file")
	mode := flag.String("mode", "live", "running mode: live or backtest")
	dataPath := flag.String("data", "", "path to historical data file for backtesting")
	symbol := flag.String("symbol", "", "symbol to backtest (e.g., BNBUSDT)")
	startDate := flag.String("start", "", "start date for backtesting (YYYY-MM-DD)")
	endDate := flag.String("end", "", "end date for backtesting (YYYY-MM-DD)")
	flag.Parse()

	// --- Initialize log (early) ---
	// To enable logging during .env or configuration loading, a temporary or default logger must be initialized before other logic
	// Here we assume that InitLogger can be safely called ahead of time
	logger.InitLogger(models.LogConfig{Level: "info", Output: "console"}) // 使用一个默认配置

	// --- 加载 .env 文件 ---
	err := godotenv.Load()
	if err != nil {
		logger.S().Info("Not found .env File，It will be read from the system environment variables.")
	} else {
		logger.S().Info("successfully from .env File loading configuration.")
	}

	// --- 加载 JSON 配置 ---
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.S().Fatalf("Can't load the config file: %v", err)
	}

	// --- 使用文件中的配置重新初始化日志 ---
	logger.InitLogger(cfg.LogConfig)
	defer logger.S().Sync() // 确保在main函数退出时刷新所有缓冲的日志

	switch *mode {
	case "live":
		runLiveMode(cfg)
	case "backtest":
		finalDataPath, err := handleBacktestMode(*symbol, *startDate, *endDate, *dataPath)
		if err != nil {
			logger.S().Fatal(err)
		}
		runBacktestMode(cfg, finalDataPath)
	default:
		logger.S().Fatalf("Unknown operating mode: %s。Please choose 'live' or 'backtest'。", *mode)
	}
}

// handleBacktestMode 处理回测模式的启动逻辑，包括数据下载。
// Returns the data file path if successful, or an error if it fails.
func handleBacktestMode(symbol, startDate, endDate, dataPath string) (string, error) {
	// 检查是否需要下载数据
	shouldDownload := symbol != "" && startDate != "" && endDate != ""

	if shouldDownload {
		startTime, err1 := time.Parse("2006-01-02", startDate)
		endTime, err2 := time.Parse("2006-01-02", endDate)
		if err1 != nil || err2 != nil {
			return "", fmt.Errorf("Wrong date format，请使用 YYYY-MM-DD 格式。start: %v, end: %v", err1, err2)
		}

		// Make sure the data directory exists
		if _, err := os.Stat("data"); os.IsNotExist(err) {
			if err := os.Mkdir("data", 0755); err != nil {
				return "", fmt.Errorf("Create data Directory failed: %v", err)
			}
		}

		downloader := downloader.NewKlineDownloader()
		fileName := fmt.Sprintf("data/%s-%s-%s.csv", symbol, startDate, endDate)
		logger.S().Infof("Start downloading %s from %s arrive %s K-line data...", symbol, startDate, endDate)

		if err := downloader.DownloadKlines(symbol, fileName, startTime, endTime); err != nil {
			return "", fmt.Errorf("Failed to download data: %v", err)
		}
		return fileName, nil // 返回下载好的文件路径
	}

	// If no download is performed, a data path must be provided
	if dataPath == "" {
		return "", fmt.Errorf("Backtest mode needs to be enabled --data 或 --symbol/start/end 参数指定数据源")
	}
	return dataPath, nil
}

// runLiveMode 运行实时交易机器人
func runLiveMode(cfg *models.Config) {
	logger.S().Info("--- Start real-time trading mode ---")

	// Load the API key from the environment variables
	apiKey := os.Getenv("BINANCE_API_KEY")
	secretKey := os.Getenv("BINANCE_SECRET_KEY")
	if apiKey == "" || secretKey == "" {
		logger.S().Fatal("Error：BINANCE_API_KEY and BINANCE_SECRET_KEY Environment variables must be set.")
	}

	// 根据配置设置API URL
	var baseURL, wsBaseURL string
	if cfg.IsTestnet {
		baseURL = cfg.TestnetAPIURL
		wsBaseURL = cfg.TestnetWSURL
		logger.S().Info("Currently using Binance Testnet...")
	} else {
		baseURL = cfg.LiveAPIURL
		wsBaseURL = cfg.LiveWSURL
		logger.S().Info("Currently using Binance mainnet...")
	}
	cfg.BaseURL = baseURL
	cfg.WSBaseURL = wsBaseURL

	// 初始化交易所
	liveExchange, err := exchange.NewLiveExchange(apiKey, secretKey, cfg.BaseURL, cfg.WSBaseURL, logger.L())
	if err != nil {
		logger.S().Fatalf("Failed to initialize the exchange: %v", err)
	}

	// --- Initialize exchange settings ---
	logger.S().Info("Initializing exchange settings...")

	// 1. 设置持仓模式 (单向/双向)
	if _, err := liveExchange.GetAccountInfo(); err != nil {
		logger.S().Fatalf(" call GetAccountInfo failure: %v", err)
		return
	}

	// 1. Set position mode (One-way/Two-way)
	currentHedgeMode, err := liveExchange.GetPositionMode()
	if err != nil {
		logger.S().Fatalf("Failed to get the current position mode: %v", err)
		return
	}

	if currentHedgeMode != cfg.HedgeMode {
		logger.S().Infof("Current Position Mode (HedgeMode=%v) 与配置 (HedgeMode=%v) 不符，Trying to update...", currentHedgeMode, cfg.HedgeMode)
		if err := liveExchange.SetPositionMode(cfg.HedgeMode); err != nil {
			logger.S().Fatalf("Failed to set position mode: %v", err)
			return
		}
		logger.S().Infof("Position mode successfully updated to: HedgeMode=%v", cfg.HedgeMode)
	} else {
		logger.S().Infof("The current position mode is already set to the target mode (HedgeMode=%v)，无需更改。", cfg.HedgeMode)
	}

	// 2. 设置保证金模式 (全仓/逐仓)
	currentMarginType, err := liveExchange.GetMarginType(cfg.Symbol)
	if err != nil {
		logger.S().Fatalf("Failed to get the current margin mode: %v", err)
		return
	}

	// 比较时忽略大小写
	if !strings.EqualFold(currentMarginType, cfg.MarginType) {
		logger.S().Infof("Current margin model (%s) With configuration (%s) 不符，Trying to update...", currentMarginType, cfg.MarginType)
		if err := liveExchange.SetMarginType(cfg.Symbol, cfg.MarginType); err != nil {
			logger.S().Fatalf("Failed to set margin mode: %v", err)
			return
		}
		logger.S().Infof("The margin mode has been successfully updated to: %s", cfg.MarginType)
	} else {
		logger.S().Infof("The current margin model is already the target model (%s)，No need to change.", cfg.MarginType)
	}

	// 初始化机器人
	gridBot := bot.NewGridTradingBot(cfg, liveExchange, false)

	// 启动机器人
	if err := gridBot.Start(); err != nil {
		logger.S().Fatalf("The bot failed to start: %v", err)
		return
	}

	// Wait for interrupt signal to perform graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// 停止机器人, 状态保存逻辑已移至 bot.Stop() 内部
	gridBot.Stop()
	logger.S().Info(""The bot has been successfully stopped.")
}

// runBacktestMode 运行回测模式
func runBacktestMode(cfg *models.Config, dataPath string) {
	logger.S().Info("--- Start backtesting mode ---")
	cfg.WSBaseURL = "ws://localhost" // In backtesting, we don't need a real WS connection.

	// 从数据路径中提取 symbol，并用它来覆盖 config 中的值
	backtestSymbol := extractSymbolFromPath(dataPath)
	if backtestSymbol == "" {
		logger.S().Fatalf("Can't access from the data file path %s 中提取交易对", dataPath)
	}
	cfg.Symbol = backtestSymbol // 确保机器人逻辑也使用正确的 symbol

	// 使用新的构造函数，并传入完整的 config
	backtestExchange := exchange.NewBacktestExchange(cfg)
	gridBot := bot.NewGridTradingBot(cfg, backtestExchange, true)

	// Load and process historical data
	file, err := os.Open(dataPath)
	if err != nil {
		logger.S().Fatalf("Can't open the historical data file: %v", err)
	}
	defer file.Close()

	// --- 重构数据读取以捕获时间 ---
	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		logger.S().Fatalf("Can't read all CSV records: %v", err)
	}
	if len(records) <= 1 { // 至少需要表头和一行数据
		logger.S().Fatal("The historical data file is empty or only has a header.")
	}

	// 移除表头
	records = records[1:]

	// Analysis start and end time
	startTimeMs, _ := strconv.ParseInt(records[0][0], 10, 64)
	endTimeMs, _ := strconv.ParseInt(records[len(records)-1][0], 10, 64)
	startTime := time.UnixMilli(startTimeMs)
	endTime := time.UnixMilli(endTimeMs)

	// --- 使用第一行数据进行初始化 ---
	initialRecord := records[0]
	initialTimeMs, _ := strconv.ParseInt(initialRecord[0], 10, 64)
	initialTime := time.UnixMilli(initialTimeMs)
	initialOpen, errO := strconv.ParseFloat(initialRecord[1], 64)
	initialHigh, errH := strconv.ParseFloat(initialRecord[2], 64)
	initialLow, errL := strconv.ParseFloat(initialRecord[3], 64)
	initialClose, errC := strconv.ParseFloat(initialRecord[4], 64)
	if errO != nil || errH != nil || errL != nil || errC != nil {
		logger.S().With(
			zap.Error(errO),
			zap.Error(errH),
			zap.Error(errL),
			zap.Error(errC),
		).Fatal("Can't parse the initial price")
	}

	backtestExchange.SetPrice(initialOpen, initialHigh, initialLow, initialClose, initialTime)
	gridBot.SetCurrentPrice(initialClose)
	if err := gridBot.StartForBacktest(); err != nil {
		logger.S().Fatalf("Backtesting bot failed to initialize: %v", err)
	}
	logger.S().Infof("使用初始价格 %.2f Robot initialization complete. \n", initialClose)

	// --- 循环处理所有数据点 ---
	logger.S().Info("开始回测...")
	for _, record := range records {
		// 检查是否爆仓或进入暂停状态
		if backtestExchange.IsLiquidated() {
			logger.S().Warn("Detected liquidation，Stop the backtesting loop early.")
			break
		}
		if gridBot.IsHalted() {
			logger.S().Info("The bot is paused，提前终止回测循环。")
			break
		}

		timestampMs, errT := strconv.ParseInt(record[0], 10, 64)
		openPrice, errO := strconv.ParseFloat(record[1], 64)
		high, errH := strconv.ParseFloat(record[2], 64)
		low, errL := strconv.ParseFloat(record[3], 64)
		closePrice, errC := strconv.ParseFloat(record[4], 64)
		if errT != nil || errO != nil || errH != nil || errL != nil || errC != nil {
			logger.S().Warnf("Can't parse K-line data，Skip this record: %v", record)
			continue
		}
		timestamp := time.UnixMilli(timestampMs)
		backtestExchange.SetPrice(openPrice, high, low, closePrice, timestamp)
		gridBot.ProcessBacktestTick()
	}

	logger.S().Info("Backtest finished.")

	// --- 生成并打印回测报告 ---
	reporter.GenerateReport(backtestExchange, dataPath, startTime, endTime)
}
