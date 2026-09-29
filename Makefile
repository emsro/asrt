
clang-tidy:
	SDKROOT=$$(xcrun --show-sdk-path 2>/dev/null || true); \
	EXTRA=; \
	if [ -n "$$SDKROOT" ]; then EXTRA="--extra-arg-before=--sysroot=$$SDKROOT"; fi; \
	find ./asrtc ./asrtl ./asrtr ./asrtio ./asrtcpp ./asrtlpp ./asrtrpp ./test \( -iname "*.h" -o -iname "*.c" -o -iname "*.cpp" -o -iname "*.hpp" \) -not -path '*/test/component/*' -not -name serial_win.cpp -not \( -path '*/test/*' \( -iname "*.h" -o -iname "*.hpp" \) \) | xargs clang-tidy -p ./_build/debug/ $$EXTRA --warnings-as-errors='*'

# Build the Docker CI image (cached after first run)
docker-image:
	docker build -t assert-ci .

# Run any preset inside the Docker CI environment, e.g. make ci-asan
ci-%: docker-image
	./docker-ci.sh $*

.PHONY: clang-tidy docker-image
