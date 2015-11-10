# Workshop Apache Spark

Workshop de Apache Spark para el "Taller Nacional de Big Data y Análisis de Redes Sociales" organizado por el ¨INECOL**



Para simplificar el proceso de instalación, este taller utiliza `docker` y en particular utiliza la `image` de `jupyter/all-spark-notebook`.

Para instalar esta imagen, utiliza

```
docker pull jupyter/all-spark-notebook`
```

Y luego de descargarlo puedes verificar que esté instalado mediante:

```
docker images
```

Si todo está correcto, para ejecutarlo usa:

```
docker run -it --rm -p 8888:8888 -v <absolute path a donde esta clonado este repo>:/home/jovyan/work --name taller-spark jupyter/all-spark-notebook
```



