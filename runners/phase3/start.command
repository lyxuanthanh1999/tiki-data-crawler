#!/bin/bash
# ==============================================================================
# Tiki Crawler Phase 3 Launcher (.command)
# Tự động chống sleep máy (caffeinate) + Tự động chạy lại nếu mất mạng
# Double-click trong Finder để chạy hoặc thêm vào 'Open at Login'
# ==============================================================================

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR" || exit 1

echo "======================================================================"
echo "🚀 [$(date '+%Y-%m-%d %H:%M:%S')] Khởi động Phase 3: Quét lỗi 83,804 ID"
echo "☕ Đang kích hoạt chế độ Caffeinate (Ngăn macOS sleep ngắt tiến trình)..."
echo "⚡ Cấu hình: Concurrency 6, Delay 1.2-2.2s (Sweet Spot Pacing ~6.2 req/s)"
echo "======================================================================"

# Vòng lặp tự động phục hồi: Tiếp tục chạy cho đến khi hoàn thành 100% (exit code 0)
while true; do
    # caffeinate -dimsu: Giữ CPU, Đĩa và Mạng luôn hoạt động khi cắm sạc
    caffeinate -dimsu ./runners/phase3/run.sh
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "======================================================================"
        echo "🎉 [$(date '+%Y-%m-%d %H:%M:%S')] TOÀN BỘ 84 BATCHES PHASE 3 ĐÃ HOÀN TẤT 100%!"
        echo "======================================================================"
        break
    elif [ $EXIT_CODE -eq 130 ]; then
        echo ""
        echo "🛑 Đã nhận lệnh dừng từ người dùng (Ctrl+C). Thoát launcher."
        break
    else
        echo "⚠️ Tiến trình kết thúc với mã lỗi $EXIT_CODE (do sleep hoặc mất mạng)."
        echo "⏳ Đang tự động kết nối và chạy tiếp sau 10 giây..."
        sleep 10
    fi
done
