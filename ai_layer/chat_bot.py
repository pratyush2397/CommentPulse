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


import faiss
import pandas as pd
from sentence_transformers import SentenceTransformer
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage
import os
from dotenv import load_dotenv

load_dotenv("config/secrets.env")

class CommentChatbot:

    def __init__(self):
        
        self.index = faiss.read_index("ai_layer/faiss_index.bin")
        self.metadata = pd.read_pickle("ai_layer/comments_metadata.pkl")
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        self.llm = ChatGroq(
        api_key=os.getenv("GROQ_API_KEY"),
        model="llama-3.3-70b-versatile")

    def search(self, question, k=10):
        
        question_embedding = self.model.encode([question])

        distances, indices = self.index.search(
        question_embedding.astype('float32'), k)

        relevant_comments = self.metadata.iloc[indices[0]]["textDisplay"].tolist()

        return relevant_comments

    def ask(self, question):
        
        comments = self.search(question)

        context = "\n".join([f"- {c}" for c in comments])

        prompt = f"""You are analyzing Indian YouTube comments about the 2026 Gulf War.
        Based on these actual comments:{context}
        Answer this question: {question}
        
        Only use information from the comments above."""

        response = self.llm.invoke([HumanMessage(content=prompt)])

        return response.content
    

if __name__ == "__main__":
    chatbot = CommentChatbot()
    
    answer = chatbot.ask("What were Indians saying about the Iran war?")
    print(answer)   
