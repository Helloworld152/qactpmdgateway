#!/bin/bash
set -e

# 获取脚本所在目录（qactpmdgateway/shells/）
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# qactpmdgateway 项目根目录
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_DIR"

PACKAGE_DIR="open-ctp-mdgateway"
TARBALL="${PACKAGE_DIR}.tar.gz"
BIN_FILE="$SCRIPT_DIR/open-ctp-mdgateway.bin"

[ ! -f "bin/open-ctp-mdgateway" ] && echo "错误: 请先运行 make" && exit 1

rm -rf "$PACKAGE_DIR" "$TARBALL" "$BIN_FILE"

mkdir -p "$PACKAGE_DIR"/{config,libs,logs}

cp bin/open-ctp-mdgateway "$PACKAGE_DIR/"
cp config/*.json "$PACKAGE_DIR/config/" 2>/dev/null || true
[ -d "libs" ] && cp libs/*.so "$PACKAGE_DIR/libs/" 2>/dev/null || true

# 使用 qactpmdgateway/shells 目录中的运行脚本
if [ -f "$SCRIPT_DIR/run_market_data.sh" ]; then
    cp "$SCRIPT_DIR/run_market_data.sh" "$PACKAGE_DIR/run.sh"
    chmod +x "$PACKAGE_DIR/run.sh"
else
    echo "错误: 未找到 shells/run_market_data.sh"
    exit 1
fi

tar czf "$TARBALL" "$PACKAGE_DIR"

cat > "$BIN_FILE" << 'INSTALL_EOF'
#!/bin/bash
set -e

ARCHIVE_LINE=$(awk '/^__ARCHIVE_BELOW__/ {print NR + 1; exit 0; }' "$0")
DEFAULT_INSTALL_DIR="$(pwd)"

INSTALL_DIR="${1:-$DEFAULT_INSTALL_DIR}"
INSTALL_DIR="$(readlink -f "$INSTALL_DIR")"

echo "安装目录: $INSTALL_DIR"
mkdir -p "$INSTALL_DIR"

tail -n +$ARCHIVE_LINE "$0" | tar xz -C "$INSTALL_DIR"

echo "安装完成: $INSTALL_DIR/qactpmdgateway"
echo "运行: cd $INSTALL_DIR/qactpmdgateway && ./run.sh"

exit 0
__ARCHIVE_BELOW__
INSTALL_EOF

cat "$TARBALL" >> "$BIN_FILE"
chmod +x "$BIN_FILE"

rm -rf "$PACKAGE_DIR" "$TARBALL"

echo "打包完成: $BIN_FILE"
echo "使用方法: ./open-ctp-mdgateway.bin [安装路径]"

