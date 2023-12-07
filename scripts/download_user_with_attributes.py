import looker_sdk
from looker_sdk.sdk.api40 import models
import pandas as pd

class UserAttributeDownloader:
    def __init__(self):
        self.user_attribute_id = 103
        self.user_ids = [10,763,242,1951,1620]
        self.sdk = looker_sdk.init40()
        
    def dump_users(self):
        # first get a dump of all users
        user_dump = []
        for user_id in self.user_ids:
            user_dump.append(self.sdk.search_users(fields="id, first_name, last_name,email,is_iam_admin",id=user_id)[0])
        # pprint(user_dump)
                        
        user_content = [
            {
                "user_id": user.id,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "email": user.email,
                "is_iam_admin":user.is_iam_admin
            }
            for user in user_dump
        ]
        for user_obj in user_content:
            user_id = user_obj['user_id']
            resp = self.sdk.user_attribute_user_values(
                user_id=f"{user_id}",
                fields="name,value,user_attribute_id",
                user_attribute_ids=models.DelimSequence([self.user_attribute_id]))[0]
            user_obj['user_attribute_name'],user_obj['user_attribute_id'],user_obj['user_attribute_value'] = resp.name,self.user_attribute_id,resp.value

        return pd.DataFrame(user_content)

