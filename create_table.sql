CREATE TABLE cambios_divisa (
    start_date DATE,
    end_date DATE,
    source VARCHAR,
    EUR VARCHAR(25),
    GBP VARCHAR(25),
    USD VARCHAR(25),
    PEN VARCHAR(25),
    BTC VARCHAR(25),
    KRW VARCHAR(25),
    ING VARCHAR(25),
    CNY VARCHAR(25),
    BRL VARCHAR(25),
    ARS VARCHAR(25),
    JPY VARCHAR(25),
    change DECIMAL(18, 2),
    ptc_change DECIMAL(18, 2)
);
