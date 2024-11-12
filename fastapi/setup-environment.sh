#!/bin/bash

# Crear un entorno virtual
python3 -m venv venv

# Activar el entorno virtual
source venv/bin/activate

# Instalar las librerias desde requirements.txt
pip install -r requirements.txt

echo "Entorno configurado"