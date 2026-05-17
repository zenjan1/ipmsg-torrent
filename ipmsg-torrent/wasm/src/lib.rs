use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub fn version() -> String {
    format!("ipmsg-t wasm v{}", env!("CARGO_PKG_VERSION"))
}
