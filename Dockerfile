FROM python
ENV LISTEN_PORT=80
EXPOSE 80
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
# Shell form (/bin/sh)
# CMD python -u npi_app.py
# Exec form
CMD ["waitress-serve","--listen=*:80", "npi_app:npi_app"]