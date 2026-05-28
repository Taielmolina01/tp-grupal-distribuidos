# Query 5

El primer filter podría ahorrar mensajes hacia el fetcher si cada instancia solo le envia cuando no le envio antes una fecha nueva. Esto mejora porque siendo N la cantidad de transacciones que pasan ese primer filter, el fetcher recibe N transacciones. En el peor de los casos que las transacciones sean de todos días distintos la performance sería la misma pero en el caso promedio (y por lo observado en el dataset), el volumen por día es bstante grande, asique siendo F la cantidad de fechas distintas, M la cantidad de filters payment_type X date y siendo que no vamos a coordinar para no enviarle repetidos al fetcher vamos a enviar M x F mensajes a través de RabbitMQ vs N mensajes.

Siendo que M << N y F << N -> MxF << N.

Manejo de archivos:
- Hacer una carpeta por cliente y luego mirar todos los archivos y deletear toda la carpeta y listo. Menos quilombo para leer, me ahorro los archivos temporales, etc. Además de que ahora mismo estoy levantando todas las N-F filas no filtradas (N = total filas, F=filtradas).
- Tema mutexes, debería tener un mutex por archivo.


