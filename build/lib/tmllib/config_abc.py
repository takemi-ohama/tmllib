from enum import Enum
from pydantic import BaseModel
from typing import Optional


class AccountType(str, Enum):
    ssm = 'ssm'
    file = 'file'
    env = 'env'


class BaseConfig(BaseModel):
    account_type: AccountType
    json_key: str
    project_id: str
    is_debug: bool
    aws_region: Optional[str]
    gbq_location: Optional[str]
    app_list: Optional[str]
