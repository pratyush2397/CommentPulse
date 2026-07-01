import logging
import os
import sys
from dotenv import load_dotenv

os.environ["PYSPARK_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from bronze.batch_ingest import create_spark_session

load_dotenv("config/secrets_template.env")
bronze_path = os.getenv("BRONZE_PATH")
silver_path = os.getenv("SILVER_PATH")
gold_path = os.getenv("GOLD_PATH")

spark = create_spark_session()

class EmbeddingBuilder:

    def __init__(self,spark,gold_path):
        self.spark = spark
        self.gold_path = gold_path

    def read_from_gold(self):

        df = self.spark.read.format("delta").load(f"{self.gold_path}/dim_comments_text")

        df = df.filter(F.col("language_category").isin(["hindi","hinglish_or_english"]))

        return df

    def create_embeddings(self,df):
        
        from sentence_transformers import SentenceTransformer

        # Convert Spark DataFrame to pandas for embedding
        pandas_df = df.select("comments_sk", "textDisplay", "war_period").toPandas()

        # Load embedding model
        model = SentenceTransformer("all-MiniLM-L6-v2")

        # Create embeddings
        embeddings = model.encode(pandas_df["textDisplay"].tolist(), show_progress_bar=True)
    
        return embeddings, pandas_df

    def save_faiss(self,embeddings,pandas_df):

        import faiss
        import numpy as np
        import pickle

        # Build FAISS index
        dimension = embeddings.shape[1]  # 384 for MiniLM
        index = faiss.IndexFlatL2(dimension)
        index.add(embeddings.astype('float32'))

        # Save index
        faiss.write_index(index, "ai_layer/faiss_index.bin")

        # Save metadata (comment text + war_period for retrieval)
        pandas_df.to_pickle("ai_layer/comments_metadata.pkl")

    def build(self):

        df = self.read_from_gold()
        embeddings, pandas_df = self.create_embeddings(df)
        self.save_faiss(embeddings, pandas_df)

if __name__ == "__main__":

    builder = EmbeddingBuilder(spark=spark, gold_path=gold_path)
    builder.build()

        