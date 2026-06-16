# Imagen base oficial de Python
FROM python:3.12-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar dependencias primero (aprovecha cache de Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del proyecto
COPY . .

# Comando por defecto al iniciar el contenedor
CMD ["python", "src/etl_pipeline.py"]
