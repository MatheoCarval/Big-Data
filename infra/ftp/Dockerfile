FROM python:3.12-slim

RUN pip install --no-cache-dir pyftpdlib==1.5.10

COPY serveur.py /opt/ftp/serveur.py

EXPOSE 21 30000-30009
CMD ["python", "-u", "/opt/ftp/serveur.py"]
