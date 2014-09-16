# Yet another MeCab tokenizer plugin for Groonga

このトークナイザープラグインは、原則、ビルトインのTokenMecabトークナイザー等と同様のルールで文字列をトークナイズします。
トークナイザープラグインの登録には、``register``コマンドを利用します。

```
register tokenizers/yamecab
```

作成中。

## Install

### Source install

Build this tokenizer.

    % sh autogen.sh
    % ./configure
    % make
    % sudo make install

## Dependencies

* Groonga >= 4.0.5

Install ``groonga-devel`` in CentOS/Fedora. Install ``libgroonga-dev`` in Debian/Ubutu.

See http://groonga.org/docs/install.html

## Usage

Firstly, register `tokenizers/yamecab`

Groonga:

    % groonga db
    > register tokenizers/yamecab
    > table_create Diaries TABLE_HASH_KEY INT32
    > column_create Diaries body COLUMN_SCALAR TEXT
    > table_create Terms TABLE_PAT_KEY ShortText \
    >   --default_tokenizer TokenYaMecab
    > column_create Terms diaries_body COLUMN_INDEX|WITH_POSITION Diaries body

Mroonga:

    mysql> use db;
    mysql> select mroonga_command("register tokenizers/yamecab");
    mysql> CREATE TABLE `Diaries` (
        -> id INT NOT NULL,
        -> body TEXT NOT NULL,
        -> PRIMARY KEY (id) USING HASH,
        -> FULLTEXT INDEX (body) COMMENT 'parser "TokenYaMecab"'
        -> ) ENGINE=mroonga DEFAULT CHARSET=utf8;

Rroonga:

    irb --simple-prompt -rubygems -rgroonga
    >> Groonga::Context.default_options = {:encoding => :utf8}   
    >> context = Groonga::Context.new
    >> Groonga::Database.create(:path => "/tmp/db", :context => context)
    >> context.register_plugin(:path => "tokenizers/yamecab.so")
    >> Groonga::Schema.create_table("Diaries",
    ?>                              :type => :hash,
    ?>                              :key_type => :integer32,
    ?>                              :context => context) do |table|
    ?>   table.text("body")
    >> end
    >> Groonga::Schema.create_table("Terms",
    ?>                              :type => :patricia_trie,
    ?>                              :normalizer => :NormalizerAuto,
    ?>                              :default_tokenizer => "TokenYaMecab",
    ?>                              :context => context) do |table|
    ?>   table.index("Diaries.body")
    >> end
    
## Author

* Naoya Murakami <naoya@createfield.com>

## License

LGPL 2.1. See COPYING for details.

This is programmed based on the Groonga MeCab tokenizer.  
https://github.com/groonga/groonga/blob/master/plugins/tokenizers/mecab.c

This program is the same license as Groonga.

The Groonga MeCab tokenizer is povided by ``Copyright(C) 2009-2012 Brazil``
