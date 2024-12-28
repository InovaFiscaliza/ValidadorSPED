LABEL org.opencontainers.image.title Validador Tributário

LABEL org.opencontainers.image.description Valida diversos arquivos tributários recebidos em uma pasta

FROM python:3

WORKDIR /usr/src/app

COPY pyproject.toml ./

RUN uv pip install -r pyproject.toml

COPY . .

CMD [ "./validadorSPED" ]
