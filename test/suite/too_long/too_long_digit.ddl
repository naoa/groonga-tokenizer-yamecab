register tokenizers/yamecab

table_create Entries TABLE_NO_KEY
column_create Entries body COLUMN_SCALAR LongText
table_create Terms TABLE_PAT_KEY ShortText --default_tokenizer TokenYaMecab
column_create Terms document_index COLUMN_INDEX|WITH_POSITION Entries body
load --table Entries
[
{"body":
}
]