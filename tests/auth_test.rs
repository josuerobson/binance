use binance_momentum::binance::auth::sign;

#[test]
fn official_binance_hmac_vector() {
    let secret = concat!(
        "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3",
        "UZjInClVN65XAbvqqM6A7H5fATj0j"
    );
    let payload = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
    let expected = "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71";
    assert_eq!(sign(secret, payload), expected);
}
