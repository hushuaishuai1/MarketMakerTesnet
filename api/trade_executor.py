from api.binance_connector import client

def execute_trade(symbol, signal):
    """
    Ejecuta una operación de compra o venta en Binance según la señal dada.
    signal: 1 para compra, -1 para venta
    """
    try:
        if signal == 1:
            # Ejecutar compra
            order = client.order_market_buy(symbol=symbol, quantity=0.001)
            print(f"Compra ejecutada para {symbol}")
            return order

        elif signal == -1:
            # Ejecutar venta
            order = client.order_market_sell(symbol=symbol, quantity=0.001)
            print(f"Venta ejecutada para {symbol}")
            return order

        else:
            print("No se ejecutó ninguna operación.")
            return None

    except Exception as e:
        print(f"Error ejecutando operación: {e}")
        return None

