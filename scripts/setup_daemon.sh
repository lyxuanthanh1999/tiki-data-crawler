#!/bin/bash
# ==============================================================================
# macOS LaunchAgent Manager for Tiki Crawler
# Tự động hóa đăng ký / gỡ bỏ dịch vụ chạy ngầm khi mở máy trên macOS
# ==============================================================================

PLIST_NAME="com.tiki.crawler.plist"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
PLIST_PATH="$LAUNCH_AGENTS_DIR/$PLIST_NAME"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_SCRIPT="$PROJECT_DIR/run_all.sh"
LOGS_DIR="$PROJECT_DIR/logs"

mkdir -p "$LAUNCH_AGENTS_DIR"
mkdir -p "$LOGS_DIR"

action="${1:-help}"

case "$action" in
    install)
        echo "⚙️ Đang cấu hình macOS LaunchAgent: $PLIST_NAME..."
        cat <<EOF > "$PLIST_PATH"
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.tiki.crawler</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>$RUN_SCRIPT</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$PROJECT_DIR</string>
    <!-- 1. Tự khởi chạy khi mở máy / đăng nhập vào macOS -->
    <key>RunAtLoad</key>
    <true/>
    <!-- 2. Tự phục hồi khi bị crash hoặc mất mạng; dừng khi hoàn thành tất cả (exit 0) -->
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>ThrottleInterval</key>
    <integer>15</integer>
    <!-- 3. Ghi log dịch vụ -->
    <key>StandardOutPath</key>
    <string>$LOGS_DIR/daemon.log</string>
    <key>StandardErrorPath</key>
    <string>$LOGS_DIR/daemon_err.log</string>
</dict>
</plist>
EOF
        echo "✅ Đã tạo file plist tại: $PLIST_PATH"

        # Kích hoạt daemon
        launchctl unload "$PLIST_PATH" 2>/dev/null
        launchctl load "$PLIST_PATH"
        echo "🚀 Đã kích hoạt macOS Daemon! Từ giờ mỗi khi mở máy, crawler sẽ tự chạy ngầm."
        echo "📊 Xem log tổng: tail -f $LOGS_DIR/daemon.log"
        ;;

    start)
        echo "▶️ Bắt đầu daemon..."
        launchctl load "$PLIST_PATH" 2>/dev/null
        launchctl start com.tiki.crawler 2>/dev/null
        echo "✅ Đã gửi lệnh start."
        ;;

    stop)
        echo "⏹️ Đang dừng daemon..."
        launchctl unload "$PLIST_PATH" 2>/dev/null
        "$PROJECT_DIR/stop_all.sh"
        echo "✅ Đã dừng daemon."
        ;;

    status)
        echo "🔍 Trạng thái dịch vụ macOS launchd:"
        launchctl list | grep "com.tiki.crawler" || echo "❌ Dịch vụ com.tiki.crawler chưa được load."
        echo ""
        echo "🔍 Các tiến trình crawler đang chạy:"
        pgrep -fl "main.py.*--start-batch" || echo "Không có tiến trình main.py nào đang chạy."
        ;;

    uninstall)
        echo "🧹 Đang gỡ bỏ macOS LaunchAgent..."
        launchctl unload "$PLIST_PATH" 2>/dev/null
        rm -f "$PLIST_PATH"
        "$PROJECT_DIR/stop_all.sh"
        echo "✅ Đã gỡ bỏ daemon hoàn toàn."
        ;;

    *)
        echo "Usage: $0 {install|start|stop|status|uninstall}"
        echo "  install   : Tạo cấu hình plist và kích hoạt tự chạy khi mở máy"
        echo "  start     : Khởi động dịch vụ thủ công"
        echo "  stop      : Tạm dừng dịch vụ và tắt các tiến trình đang chạy"
        echo "  status    : Kiểm tra trạng thái dịch vụ và các tiến trình"
        echo "  uninstall : Hủy kích hoạt và xóa file plist khỏi macOS"
        exit 1
        ;;
esac
