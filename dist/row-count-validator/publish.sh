#!/usr/bin/env bash
# =============================================================================
# publish.sh — Build the Row Count Validator self-contained binary (Linux/macOS)
#
# Usage:
#   chmod +x publish.sh
#   ./publish.sh                    # defaults to linux-x64
#   ./publish.sh osx-x64            # macOS Intel
#   ./publish.sh osx-arm64          # macOS Apple Silicon (M1/M2/M3)
#   ./publish.sh win-x64            # Windows binary (cross-compile from macOS/Linux)
#
# Output:
#   ./output/<rid>/row-count-validator[.exe]
#   ./output/<rid>/.env.template
#   ./output/<rid>/README-BAU.md
#
# Requirements:
#   - .NET 9 SDK installed (https://dot.net)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT="$REPO_ROOT/src/Ora2PgRowCountValidator/Ora2PgRowCountValidator.csproj"

RID="${1:-linux-x64}"
OUT_DIR="$SCRIPT_DIR/output/$RID"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Row Count Validator — Publishing for: $RID"
echo "═══════════════════════════════════════════════════"
echo ""

# Clean output
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

# Publish
dotnet publish "$PROJECT" \
  -c Release \
  -r "$RID" \
  --self-contained true \
  -p:PublishSingleFile=true \
  -p:IncludeNativeLibrariesForSelfExtract=true \
  -p:EnableCompressionInSingleFile=true \
  -o "$OUT_DIR"

# Remove dev artifacts that BAU doesn't need
rm -f "$OUT_DIR"/*.pdb
rm -f "$OUT_DIR"/*.xml

# Copy BAU support files
cp "$SCRIPT_DIR/.env.template"  "$OUT_DIR/.env"
cp "$SCRIPT_DIR/README.md"      "$OUT_DIR/README.md"

# Rename executable to a friendly name
if [[ "$RID" == win-* ]]; then
  mv "$OUT_DIR/Ora2PgRowCountValidator.exe" "$OUT_DIR/row-count-validator.exe" 2>/dev/null || true
else
  mv "$OUT_DIR/Ora2PgRowCountValidator" "$OUT_DIR/row-count-validator" 2>/dev/null || true
  chmod +x "$OUT_DIR/row-count-validator"
fi

echo ""
echo "✅ Done! Package ready at: $OUT_DIR"
echo ""
echo "Contents:"
ls -lh "$OUT_DIR"
echo ""
echo "Zip for delivery:"
echo "  cd $SCRIPT_DIR/output && zip -r row-count-validator-$RID.zip $RID/"
echo ""
