FROM python:3.11-slim

WORKDIR /app

RUN pip install --upgrade pip && \
    pip install streamlit pandas s3fs pyarrow

COPY . /app

EXPOSE 8501

<<<<<<< HEAD
CMD ["streamlit", "run", "app.py"]
=======
CMD ["streamlit", "run", "app.py"]
>>>>>>> 668b11879f30693ef554dceaa5d53624d3f2f9ed
