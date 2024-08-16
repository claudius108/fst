wasm-pack build --release --target web
cp ./pkg/*.js ../public
cp ./pkg/*_bg.wasm ../public
