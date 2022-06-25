from nautilus_trader.core.rust.model cimport instrument_new
from nautilus_trader.core.rust.model cimport Instrument_t
from nautilus_trader.core.rust.model cimport Price_t
from nautilus_trader.model.objects cimport Price
from nautilus_trader.model.objects cimport Quantity
from cpython.object cimport PyObject

cpdef void main():

    cdef Price price = Price(0.2343, 4);
    cdef Quantity quantity = Quantity(0.9, 1);
    cdef Instrument_t instrument = instrument_new(
                                        Price.as_option(price),
                                        Quantity.as_option(quantity))
    print(instrument)
    price = None
    quantity = None
    instrument = instrument_new(
                    Price.as_option(price),
                    Quantity.as_option(quantity))
    print(instrument)

