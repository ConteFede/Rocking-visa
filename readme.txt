A prueba y error generé el primer ETL, pudo ser mas prolijo, limpiar mas data de relleno, usado mas librerías y metodos nuevos (aunque para mí son todos nuevos...)
1) Creeamos con SQL las database y usuarios con acceso
2) Generamos el motor de conexión en python con el método create_engine
3) Con with open revelamos el coding del dataset
4) Extraemos del csv y guardamos como está en el stg del server. Hay un método que encontré, no estoy seguro que funcione, para guardar las columnas con error. (columns_errors)
5) Empezamos a ver la composición del archivo, tipo de dato y cantidades de no nulos
6) Empieza la pelea con las ciudades/estado. Creamos la tabla donde empezamos a transformar su data.
7) Viendo que en worksite estan todas las filas completas, separamos ciudad de estado y transformamos todo en mayus con apply y reemplazamos dos comas por una, normalizando. Guardamos en el server
8) Tuve la intención de comparar con un método interesante y rescatar ciudades comparando con una base de ciudades de eeuu, la descargue limpie y comparé con SequenceMatcher, duró la pelea unos días, opté por dejarlo ganar y seguir avanzando. (volveré)
9) Limpiamos eliminando duplicados de algunas variables categoricas concatenadas, le damos un key y se lo insertamos en la tabla principal. De esta forma cuando lo tengamos en el dw, tenemos muchos menos datos usando la tabla principal, teniendo una key a toda la parte empleo, igual que ciudad.
10) Vemos un poco el sueldo, eliminamos duplicados de case, me queda pendiente ver si los duplicados elimnados puedan tener distinto dato, son menos de un 5% del total de datos, podemos considerar poco significativo, de todas formas debe haber algo facil para comparar. 
10.1) Tenemos la carga de la estructura del DW, con las pk, fk, hubo unas cuantas piñas, pero ganamos.
11) cargamos en 3 partes el DW, lo hubiese hecho en una, pero temí por la integridad de mi ram, y así funcionó.

PD: Muy útil el curso, me gustó y aprendí, ya empecé a usarlo en el laburo, fui probando alejarme del excel, y funciona. Gracias.

Se que el camino con python es un mundo por recorrer, pero con este curso ya me enseñaron a caminar.
