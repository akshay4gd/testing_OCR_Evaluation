from pymongo import MongoClient
import requests
import pandas as pd

# MongoDB connection string
mongo_uri = "mongodb://pesudev1:18GWD14sa%7Dy0Y%3A9Tt%5EcL23!%40!@44.198.39.3/ParallelProcessingTesting_MOF?authSource=admin"
client = MongoClient(mongo_uri)

# Database and collection names
db_name = "ParallelProcessingTesting_MOF"
answers_collection_name = "answers"
questions_collection_name = "questions"  # Assuming the question collection name is 'question'
grades_collection_name = "grades"

# Connect to the database and collections
db = client[db_name]
answers_collection = db[answers_collection_name]
questions_collection = db[questions_collection_name]
grades_collection = db[grades_collection_name]



# Function to fetch data for the specific QuestionId and send it to the pesu-evaluation API
def fetch_and_evaluate():
    # Define the specific QuestionId
    specific_question_id = "b54324fd-b45e-447c-ba31-1e4bcf10465e"
    
    # Fetch all documents with the specific QuestionId from the answers collection
    answers = answers_collection.find({"QuestionId": specific_question_id})

    # Fetch the corresponding question document from the question collection
    question = questions_collection.find_one({"QuestionId": specific_question_id})

    if not question:
        print(f"No question found with QuestionId: {specific_question_id}")
        return

    # Prepare an empty list to store rows for the DataFrame
    data_rows = []

    for answer in answers:

        # Fetch old OCR and old AI details
        old_ocr = answer.get("StudentAnswerOCR", "")
        student_id = answer.get("StudentId")
        question_id = answer.get("QuestionId")

        # Fetch old AI score and feedback from the grades collection
        old_grade = grades_collection.find_one({"StudentId": student_id, "QuestionId": question_id})
        old_ai_score = old_grade.get("AIScore", "") if old_grade else ""
        old_ai_feedback = old_grade.get("AiFeedback", "") if old_grade else ""

        # Construct the payload to send to the pesu-evaluation API
        eval_request = {
            "SubjectName": question.get("Test", ""),  # Extracting from question collection
            "QuestionId": question.get("QuestionId", ""),  # Extracting from question collection
            "Rubric": question.get("Rubric", ""),  # Extracting from question collection
            "RAG_context": question.get("RAG_context", ""),  # Extracting from question collection
            "Prompt_payload": question.get("Prompt_payload", ""),  # Extracting from question collection
            "StudentId": answer.get("StudentId"),
            "StudentAnswer": answer.get("StudentAnswer", ""),
            "StudentAnswerImage": answer.get("EnhancedStudentAnswerImage", ""),
            "TotalPoints": question.get("TotalPoints", "")  # Extracting from question collection
        }

        # Send the request to the pesu-evaluation API
        try:
            response = requests.post("http://localhost:8002/generate_eval", json=eval_request)
            response_data = response.json()

            # Extract AIScore and AiFeedback from the response
            ai_score = response_data.get("AIScore")
            ai_feedback = response_data.get("AIFeedback")

            print("ai_score = ",ai_score)
            # Update the grades collection with the AIScore and AiFeedback

            grades_collection.update_one(
                {"StudentId": eval_request["StudentId"], "QuestionId": eval_request["QuestionId"]},
                {"$set": {"AiScore": ai_score, "AiFeedback": ai_feedback}},
                upsert=True
            )

            print(f"Evaluation successful for StudentId: {eval_request['StudentId']} and QuestionId: {eval_request['QuestionId']}")

            #  Append the data to the list for the DataFrame
            data_rows.append({
                "QuestionID": question_id,
                "StudentID": student_id,
                "oldOCR": old_ocr,
                "EnhancedOCR": eval_request["StudentAnswer"],  # Assuming StudentAnswer is the EnhancedOCR
                "oldAIscore": old_ai_score,
                "AiScore": ai_score,
                "oldAIFeedback": old_ai_feedback,
                "AiFeedback": ai_feedback
            })
        
            print({
                    "QuestionID": question_id,
                    "StudentID": student_id,
                    "oldOCR": old_ocr,
                    "EnhancedOCR": eval_request["StudentAnswer"],  # Assuming StudentAnswer is the EnhancedOCR
                    "oldAIscore": old_ai_score,
                    "AiScore": ai_score,
                    "oldAIFeedback": old_ai_feedback,
                    "AiFeedback": ai_feedback
                })
        
        except Exception as e:
            print(f"Failed to evaluate for StudentId: {eval_request['StudentId']} and QuestionId: {eval_request['QuestionId']}. Error: {e}")

    # Create a DataFrame from the collected data
    df = pd.DataFrame(data_rows)
    # Export the DataFrame to an Excel file
    df.to_excel('answers_table2.xlsx', index=False)
    print(df)  # Display or use the DataFrame as needed

if __name__ == "__main__":
    fetch_and_evaluate()
