#!/bin/bash
# ==============================================================================
# Tiki Crawler Launcher (.command)
# Double-click hoặc thêm vào Open at Login để tự chạy khi mở máy
# ==============================================================================

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR" || exit 1

./runners/phase1/run.sh
