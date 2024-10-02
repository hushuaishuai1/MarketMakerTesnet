# Changelog


[v1.0.0] - 2024-09-20
# Added
Implementación de la estrategia de momentum para BTCUSDT.
Endpoint para obtener datos históricos.
Grid Search para optimizar los parámetros de la estrategia.
# Fixed
Evitar duplicaciones en la base de datos para señales y datos históricos.
[v1.1.0] - 2024-09-22
# Added
Endpoint para ejecutar backtesting.
Funcionalidad de no sobrescribir datos en la base de datos, aplicable a cualquier entidad.
# Fixed
Corrección en las rutas de los endpoints para momentum y datos históricos.
[v1.2.0] - 2024-09-25
# Added
Indicadores Adicionales: Integración de indicadores MACD, OBV, ATR y ADX en la estrategia de momentum.
Nueva Estrategia de Momentum: Ajustes para la estrategia de momentum incluyendo señales basadas en cruce de medias (EMA 8, EMA 23) y confirmaciones con RSI, ADX, y OBV.
Backtesting Mejorado: Se implementó la función run_backtesting para permitir backtesting de estrategias a través de la API.
Soporte para Temporalidad Superior: Se agregó el cálculo de EMA 50 y EMA 200 en una temporalidad superior (4h) para la estrategia de momentum.
Endpoint para Backtesting: Se creó un endpoint dedicado para ejecutar el backtesting de una estrategia sobre cualquier par y temporalidad definida.
# Fixed
Errores de Null Values: Se corrigió el manejo de valores nulos en los indicadores ADX y DX en la estrategia de momentum para evitar warnings de pandas.
Compatibilidad con Pandas: Solución al error de combinación entre índices con y sin zona horaria en las uniones entre temporalidades.
Errores con Tipos de Datos: Solución al error que trataba las columnas close, high, y low como strings en lugar de floats, lo que generaba fallos en los cálculos de los indicadores.
Error en Backtesting: Solución al problema de to_pydatetime con columnas que contenían valores int en lugar de datetime.
[v1.2.1] - 2024-09-26
# Added
Migraciones de Base de Datos: Actualización de la base de datos para incluir nuevos campos  volume, etc.) en el modelo Signal.
# Fixed
Importaciones Incorrectas: Se corrigieron importaciones faltantes como requests en varios archivos.
Errores en la Función de Backtesting: Solución al error que requería pasar los argumentos de symbol e interval correctamente en el backtesting.
Manejo de Datos NaN: Se implementó el relleno con ffill para asegurar que no haya valores NaN en los cálculos de indicadores.
[v1.3.0] - 2024-09-27
 # Added
Nuevo Modelo de Datos: Se agregó la columna signal para representar las señales de compra/venta en el modelo Signal.
Validación de Datos: Se mejoró la validación de los datos antes de guardarlos en la base de datos para prevenir errores de tipo.
API Mejorada para la Estrategia Momentum: Se añadió una verificación más robusta de los indicadores calculados (como MACD, OBV, y ADX) para asegurar su correcta ejecución y almacenamiento.
## [v1.3.0] - 2024-09-25
### Added
- **Trailing Stop**: Se implementó un Trailing Stop dinámico para mejorar la gestión de riesgo en la estrategia de momentum.
- **Optimización de Parámetros**: Se ajustaron los parámetros de **EMA**, **RSI**, **ATR**, y **ADX** para hacer la estrategia más flexible y menos restrictiva.
- **Métricas Adicionales**: Se añadieron métricas adicionales como:
  - **Retorno Anualizado**
  - **Tasa de Aciertos**
  - **Drawdown Máximo**
  - **Factor de Recuperación**
- **Ajuste en Condiciones de Entrada/Salida**: Se flexibilizaron las condiciones para las señales de compra y venta, permitiendo más operaciones.
  
### Fixed
- **Problemas de Operaciones Nulas**: Se solucionó el problema donde las señales de compra/venta no se generaban correctamente debido a condiciones demasiado restrictivas.
- **Lógica de Backtesting**: Mejoras en la ejecución de operaciones de compra y venta dentro de la simulación para reflejar correctamente el rendimiento de la estrategia.
  
### Improved
- **Ajuste de ATR**: Se ajustaron los multiplicadores de **ATR** para mejorar la precisión de los niveles de **stop-loss** y **take-profit**.
- **Rendimiento del Grid Search**: Se mejoró la búsqueda de parámetros óptimos al flexibilizar las condiciones y reducir el sobreajuste.