
use crate::identifiers::instrument_id::InstrumentId;
use crate::types::currency::Currency;
use crate::types::price::Price;
use crate::types::price::Option_Price;
use crate::types::quantity::Quantity;
use crate::types::quantity::Option_Quantity;
use pyo3::{ffi, FromPyPointer, IntoPyPointer, Py, Python, PyObject, PyObjectProtocol};
use std::fmt::{Debug, Display, Formatter, Result};

#[repr(C)]
pub struct Instrument {
    price_increment: Option<Price>, // Can be None (if using a tick scheme)
    max_quantity: Option<Quantity>,
}

#[no_mangle]
pub unsafe extern "C" fn instrument_new(
    price_increment: Option<Price>, // Can be None (if using a tick scheme) Option_Price
    max_quantity: Option<Quantity>,
    
) -> Instrument {
    
    Instrument {
        price_increment: price_increment, // None
        max_quantity: max_quantity,
    }
    
}

