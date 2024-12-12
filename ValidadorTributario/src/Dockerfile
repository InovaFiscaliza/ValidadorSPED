LABEL org.opencontainers.image.title Validador Tributário

LABEL org.opencontainers.image.description Valida diversos arquivos tributários recebidos em uma pasta

FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./ValidadorTributario.py" ]
