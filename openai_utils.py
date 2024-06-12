import json
import random
import pandas as pd

def create_jsonl():

    with open("rbc_01.01.2010_02.06.2024.jsonl", "r", encoding="UTF-8") as f:
        content = f.read()

    new_jsonl = []

    for counter, i in enumerate(content.split("\n")[100000:150000]):
        # prepared_text = i.replace("\\", "")
        text_json = json.loads(i)

        new_jsonl_object = {
            "custom_id": "{}|{}".format(text_json["publish_date"], counter),
            "method": "POST",
            "url": "/v1/embeddings",
            "body": {
                "encoding_format": "float",
                "model": "text-embedding-3-small",
                "input": text_json["title"]
            }
        }

        s = json.dumps(new_jsonl_object, ensure_ascii=False)
        new_jsonl.append(s)

    jsonl_s = "\n".join([str(i) for i in new_jsonl])
    with open("jsonl_for_embedings3.jsonl", "w", encoding="UTF-8") as f:
        f.write(jsonl_s)

if __name__ == "__main__":
    create_jsonl()