/*
  Copyright(C) 2014 Naoya Murakami <naoya@createfield.com>

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License version 2.1 as published by the Free Software Foundation.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
  02110-1301  USA

  This file includes the Groonga MeCab tokenizer code.
  https://github.com/groonga/groonga/blob/master/plugins/tokenizers/mecab.c

  The following is the header of the file:

    Copyright(C) 2009-2012 Brazil

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License version 2.1 as published by the Free Software Foundation.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
    USA
*/

#include <groonga/tokenizer.h>

#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <mecab.h>

#ifdef __GNUC__
#  define GNUC_UNUSED __attribute__((__unused__))
#else
#  define GNUC_UNUSED
#endif

typedef enum {
  GRN_TOKEN_GET = 0,
  GRN_TOKEN_ADD,
  GRN_TOKEN_DEL
} grn_token_mode;

#define GRN_STRING_ENABLE_NORMALIZER_FILTER (0x01<<5)

static mecab_t *sole_mecab = NULL;
static grn_plugin_mutex *sole_mecab_mutex = NULL;
static grn_encoding sole_mecab_encoding = GRN_ENC_NONE;

static grn_bool is_additional_regist = GRN_FALSE;

#define TOKENIZER_PATH_NAME "tokenizers/yamecab"
#define SETTING_TABLE_NAME "@yamecab"

#define DEFAULT_MECAB_PARSE_LIMIT 1200000
#define DEFAULT_RFIND_PUNCT_OFFSET 300

typedef struct {
  mecab_t *mecab;
  grn_tokenizer_query *query;
  grn_tokenizer_token token;
  unsigned int parse_limit;
  unsigned int rfind_punct_offset;
  const mecab_node_t *node;
  const char *rest_string;
  unsigned int rest_length;
} grn_yamecab_tokenizer;

static grn_encoding
translate_mecab_charset_to_grn_encoding(const char *charset)
{
  if (strcasecmp(charset, "euc-jp") == 0) {
    return GRN_ENC_EUC_JP;
  } else if (strcasecmp(charset, "utf-8") == 0 ||
             strcasecmp(charset, "utf8") == 0) {
    return GRN_ENC_UTF8;
  } else if (strcasecmp(charset, "shift_jis") == 0 ||
             strcasecmp(charset, "shift-jis") == 0 ||
             strcasecmp(charset, "sjis") == 0) {
    return GRN_ENC_SJIS;
  }
  return GRN_ENC_NONE;
}

static grn_encoding
get_mecab_encoding(mecab_t *mecab)
{
  grn_encoding encoding = GRN_ENC_NONE;
  const mecab_dictionary_info_t *dictionary_info;
  dictionary_info = mecab_dictionary_info(mecab);
  if (dictionary_info) {
    const char *charset = dictionary_info->charset;
    encoding = translate_mecab_charset_to_grn_encoding(charset);
  }
  return encoding;
}

static int
rfind_punct(grn_ctx *ctx, grn_encoding encoding,
            const char *string, int start, int offset_limit, int end)
{
  int punct_position;
  int char_length;
  const char *string_top;
  const char *string_tail;

  if (offset_limit < start) {
    offset_limit = start;
  }
  string_top = string + offset_limit;

  for (string_tail = string + end;
       string_tail > string_top; string_tail -= char_length) {
    char_length = grn_plugin_charlen(ctx, (char *)string_tail,
                                     end - start, encoding);
    if (string_tail + char_length &&
        (ispunct(*string_tail) ||
         !memcmp(string_tail, "。", char_length) ||
         !memcmp(string_tail, "、", char_length) )) {
      break;
    }
  }
  punct_position = string_tail - string;
  if (punct_position <= start) {
    punct_position = end;
  }
  return punct_position;
}

static int
check_euc(const unsigned char *x, int y)
{
  const unsigned char *p;
  for (p = x + y - 1; p >= x && *p >= 0x80U; p--);
  return (int) ((x + y - p) & 1);
}

static int
check_sjis(const unsigned char *x, int y)
{
  const unsigned char *p;
  for (p = x + y - 1; p >= x; p--)
  if ((*p < 0x81U) || (*p > 0x9fU && *p < 0xe0U) || (*p > 0xfcU))
    break;
  return (int) ((x + y - p) & 1);
}

static int
rfind_lastbyte(GNUC_UNUSED grn_ctx *ctx, grn_encoding encoding,
               const char *string, int offset)
{
  switch (encoding) {
  case GRN_ENC_EUC_JP:
    while (!(check_euc((unsigned char *) string, offset))) {
      offset -= 1;
    }
    break;
  case GRN_ENC_SJIS:
    while (!(check_sjis((unsigned char *) string, offset))) {
      offset -= 1;
    }
    break;
  case GRN_ENC_UTF8:
    while (offset && string[offset] <= (char)0xc0) {
      offset -= 1;
    }
    break;
  default:
    break;
  }
  return offset;
}

static const mecab_node_t*
split_mecab_sparse_node(grn_ctx *ctx, mecab_t *mecab, grn_encoding encoding,
                        unsigned int parse_limit, unsigned int rfind_punct_offset,
                        const char *string, unsigned int string_length,
                        unsigned int *parsed_string_length)
{
  const mecab_node_t *node;
  if (string_length < parse_limit) {
    node = mecab_sparse_tonode2(mecab, string, string_length);
    *parsed_string_length = string_length;
  } else {
    int splitted_string_end = parse_limit;
    unsigned int splitted_string_length;
    int punct_position = 0;
    splitted_string_end = rfind_lastbyte(ctx, encoding,
                                         string,
                                         splitted_string_end);
    if (splitted_string_end == 0) {
      splitted_string_end = parse_limit;
    }
    punct_position = rfind_punct(ctx, encoding,
                                 string,
                                 0,
                                 splitted_string_end - rfind_punct_offset,
                                 splitted_string_end);
    splitted_string_length = punct_position;
    node = mecab_sparse_tonode2(mecab, string, splitted_string_length);
    *parsed_string_length = splitted_string_length;
  }
  return node;
}

static grn_obj *
yamecab_init(grn_ctx *ctx, int nargs, grn_obj **args, grn_user_data *user_data)
{
  grn_yamecab_tokenizer *tokenizer;
  unsigned int normalizer_flags = GRN_STRING_ENABLE_NORMALIZER_FILTER;
  grn_tokenizer_query *query;
  grn_obj *normalized_query;
  const char *normalized_string;
  unsigned int normalized_string_length;
  grn_obj *var;

  query = grn_tokenizer_query_open(ctx, nargs, args, normalizer_flags);
  if (!query) {
    return NULL;
  }
  if (!sole_mecab) {
    grn_plugin_mutex_lock(ctx, sole_mecab_mutex);
    if (!sole_mecab) {
      sole_mecab = mecab_new2("-Owakati");
      if (!sole_mecab) {
        GRN_PLUGIN_ERROR(ctx, GRN_TOKENIZER_ERROR,
                         "[tokenizer][yamecab] "
                         "mecab_new2() failed on yamecab_init(): %s",
                         mecab_strerror(NULL));
      } else {
        sole_mecab_encoding = get_mecab_encoding(sole_mecab);
      }
    }
    grn_plugin_mutex_unlock(ctx, sole_mecab_mutex);
  }
  if (!sole_mecab) {
    grn_tokenizer_query_close(ctx, query);
    return NULL;
  }

  if (query->encoding != sole_mecab_encoding) {
    grn_tokenizer_query_close(ctx, query);
    GRN_PLUGIN_ERROR(ctx, GRN_TOKENIZER_ERROR,
                     "[tokenizer][yamecab] "
                     "MeCab dictionary charset (%s) does not match "
                     "the table encoding: <%s>",
                     grn_encoding_to_string(sole_mecab_encoding),
                     grn_encoding_to_string(query->encoding));
    return NULL;
  }

  if (!(tokenizer = GRN_PLUGIN_MALLOC(ctx, sizeof(grn_yamecab_tokenizer)))) {
    grn_tokenizer_query_close(ctx, query);
    GRN_PLUGIN_ERROR(ctx, GRN_NO_MEMORY_AVAILABLE,
                     "[tokenizer][yamecab] "
                     "memory allocation to grn_yamecab_tokenizer failed");
    return NULL;
  }
  tokenizer->mecab = sole_mecab;
  tokenizer->query = query;

  normalized_query = query->normalized_query;
  grn_string_get_normalized(ctx,
                            normalized_query,
                            &normalized_string,
                            &normalized_string_length,
                            NULL);

  var = grn_plugin_proc_get_var(ctx, user_data, "parse_limit", -1);
  if (GRN_TEXT_LEN(var) != 0) {
    tokenizer->parse_limit = GRN_UINT32_VALUE(var);
  } else {
    tokenizer->parse_limit = DEFAULT_MECAB_PARSE_LIMIT;
  }
  var = grn_plugin_proc_get_var(ctx, user_data, "rfind_punct_offset", -1);
  if (GRN_TEXT_LEN(var) != 0) {
    tokenizer->rfind_punct_offset = GRN_UINT32_VALUE(var);
  } else {
    tokenizer->rfind_punct_offset = DEFAULT_RFIND_PUNCT_OFFSET;
  }
  grn_plugin_mutex_lock(ctx, sole_mecab_mutex);
  {
#define MECAB_PARSE_MIN 4096
    unsigned int parsed_string_length;
    grn_bool is_success = GRN_FALSE;
    while (!is_success) {
      tokenizer->node = split_mecab_sparse_node(ctx, tokenizer->mecab,
                                                tokenizer->query->encoding,
                                                tokenizer->parse_limit,
                                                tokenizer->rfind_punct_offset,
                                                normalized_string,
                                                normalized_string_length,
                                                &parsed_string_length);
      if (!tokenizer->node) {
        tokenizer->parse_limit /= 2;
        GRN_PLUGIN_LOG(ctx, GRN_LOG_NOTICE,
                       "[tokenizer][yamecab] "
                       "mecab_sparse_tonode() failed len=%d err=%s do retry",
                       parsed_string_length,
                       mecab_strerror(tokenizer->mecab));
      } else {
        tokenizer->node = tokenizer->node->next;
        tokenizer->rest_length = normalized_string_length - parsed_string_length;
        tokenizer->rest_string = normalized_string + parsed_string_length;
        is_success = GRN_TRUE;
      }
      if (tokenizer->parse_limit < MECAB_PARSE_MIN) {
        break;
      }
    }
    if (!tokenizer->node) {
      GRN_PLUGIN_ERROR(ctx, GRN_TOKENIZER_ERROR,
                       "[tokenizer][yamecab] "
                       "mecab_sparse_tonode() failed len=%d err=%s",
                       parsed_string_length,
                       mecab_strerror(tokenizer->mecab));
      grn_tokenizer_query_close(ctx, tokenizer->query);
      GRN_PLUGIN_FREE(ctx, tokenizer);
      return NULL;
    }
#undef MECAB_PARSE_MIN
  }
  grn_plugin_mutex_unlock(ctx, sole_mecab_mutex);

  user_data->ptr = tokenizer;
  grn_tokenizer_token_init(ctx, &(tokenizer->token));

  return NULL;
}

static grn_obj *
yamecab_next(grn_ctx *ctx, GNUC_UNUSED int nargs, GNUC_UNUSED grn_obj **args,
             grn_user_data *user_data)
{
  grn_yamecab_tokenizer *tokenizer = user_data->ptr;
  grn_tokenizer_status status;

/*
  GRN_LOG(ctx, GRN_LOG_WARNING, "node surface=%s", tokenizer->node->surface);
  GRN_LOG(ctx, GRN_LOG_WARNING, "node feature=%s", tokenizer->node->feature);
  GRN_LOG(ctx, GRN_LOG_WARNING, "node length=%d", tokenizer->node->length);
  GRN_LOG(ctx, GRN_LOG_WARNING, "node rlength=%d", tokenizer->node->rlength);
  GRN_LOG(ctx, GRN_LOG_WARNING, "node char_type=%d", tokenizer->node->char_type);
  GRN_LOG(ctx, GRN_LOG_WARNING, "node id=%d", tokenizer->node->id);
  GRN_LOG(ctx, GRN_LOG_WARNING, "node posid=%d", tokenizer->node->posid);
  GRN_LOG(ctx, GRN_LOG_WARNING, "node stat=%d", tokenizer->node->stat);
  GRN_LOG(ctx, GRN_LOG_WARNING, "node isbest=%d", tokenizer->node->isbest);
*/

  if (tokenizer->node->stat == MECAB_UNK_NODE) {
    //未知語
  }

  if (tokenizer->node->next &&
      !(tokenizer->node->next->stat == MECAB_BOS_NODE) &&
      !(tokenizer->node->next->stat == MECAB_EOS_NODE)) {
    status = GRN_TOKENIZER_CONTINUE;
  } else {
    if (tokenizer->rest_length) {
      status = GRN_TOKENIZER_CONTINUE;
      if (tokenizer->node->stat == MECAB_BOS_NODE ||
          tokenizer->node->stat == MECAB_EOS_NODE) {
        status |= GRN_TOKENIZER_TOKEN_SKIP_WITH_POSITION;
      }
    } else {
      status = GRN_TOKENIZER_LAST;
    }
  }
  grn_tokenizer_token_push(ctx, &(tokenizer->token),
                           tokenizer->node->surface, tokenizer->node->length, status);

  if (!tokenizer->node->next && tokenizer->rest_length) {
    grn_plugin_mutex_lock(ctx, sole_mecab_mutex);
    {
#define MECAB_PARSE_MIN 4096
      unsigned int parsed_string_length;
      grn_bool is_success = GRN_FALSE;
      while (!is_success) {
        tokenizer->node = split_mecab_sparse_node(ctx, tokenizer->mecab,
                                                  tokenizer->query->encoding,
                                                  tokenizer->parse_limit,
                                                  tokenizer->rfind_punct_offset,
                                                  tokenizer->rest_string,
                                                  tokenizer->rest_length,
                                                  &parsed_string_length);
        if (!tokenizer->node) {
          tokenizer->parse_limit /= 2;
          GRN_PLUGIN_LOG(ctx, GRN_LOG_NOTICE,
                         "[tokenizer][yamecab] "
                         "mecab_sparse_tonode() failed len=%d err=%s do retry",
                         parsed_string_length,
                         mecab_strerror(tokenizer->mecab));
        } else {
          tokenizer->rest_length -= parsed_string_length;
          tokenizer->rest_string += parsed_string_length;
          is_success = GRN_TRUE;
        }
        if (tokenizer->parse_limit < MECAB_PARSE_MIN) {
          break;
        }
      }
      if (!tokenizer->node) {
        GRN_PLUGIN_ERROR(ctx, GRN_TOKENIZER_ERROR,
                         "[tokenizer][yamecab] "
                         "mecab_sparse_tonode() failed len=%d err=%s",
                         parsed_string_length,
                         mecab_strerror(tokenizer->mecab));
      }
#undef MECAB_PARSE_MIN
    }
    grn_plugin_mutex_unlock(ctx, sole_mecab_mutex);
  }
  if (tokenizer->node->next) {
    tokenizer->node = tokenizer->node->next;
  }

  return NULL;
}

static grn_obj *
yamecab_fin(grn_ctx *ctx, GNUC_UNUSED int nargs, GNUC_UNUSED grn_obj **args,
            grn_user_data *user_data)
{
  grn_yamecab_tokenizer *tokenizer = user_data->ptr;
  if (!tokenizer) {
    return NULL;
  }
  grn_tokenizer_token_fin(ctx, &(tokenizer->token));
  grn_tokenizer_query_close(ctx, tokenizer->query);
  GRN_PLUGIN_FREE(ctx, tokenizer);
  return NULL;
}

static void
check_mecab_dictionary_encoding(GNUC_UNUSED grn_ctx *ctx)
{
#ifdef HAVE_MECAB_DICTIONARY_INFO_T
  mecab_t *mecab;

  mecab = mecab_new2("-Owakati");
  if (mecab) {
    grn_encoding encoding;
    int have_same_encoding_dictionary = 0;

    encoding = GRN_CTX_GET_ENCODING(ctx);
    have_same_encoding_dictionary = encoding == get_mecab_encoding(mecab);
    mecab_destroy(mecab);

    if (!have_same_encoding_dictionary) {
      GRN_PLUGIN_ERROR(ctx, GRN_TOKENIZER_ERROR,
                       "[tokenizer][yamecab] "
                       "MeCab has no dictionary that uses the context encoding"
                       ": <%s>",
                       grn_encoding_to_string(encoding));
    }
  } else {
    GRN_PLUGIN_ERROR(ctx, GRN_TOKENIZER_ERROR,
                     "[tokenizer][yamecab] "
                     "mecab_new2 failed in check_mecab_dictionary_encoding: %s",
                     mecab_strerror(NULL));
  }
#endif
}

static grn_bool
is_table(grn_obj *obj)
{
  switch (obj->header.type) {
  case GRN_TABLE_HASH_KEY:
  case GRN_TABLE_PAT_KEY:
  case GRN_TABLE_DAT_KEY:
  case GRN_TABLE_NO_KEY:
    return GRN_TRUE;
  default:
    return GRN_FALSE;
  }
}

static grn_bool
is_column(grn_obj *obj)
{
  switch (obj->header.type) {
  case GRN_COLUMN_FIX_SIZE:
  case GRN_COLUMN_VAR_SIZE:
  case GRN_COLUMN_INDEX:
    return GRN_TRUE;
  default:
    return GRN_FALSE;
  }
}

static grn_obj *
command_yamecab_register(grn_ctx *ctx, GNUC_UNUSED int nargs,
                         GNUC_UNUSED grn_obj **args,
                         GNUC_UNUSED grn_user_data *user_data)
{
  if (nargs) {
    grn_obj *obj;
    obj = args[0];
    if (is_table(obj) || is_column(obj)) {
      GNUC_UNUSED grn_obj *flags = grn_ctx_pop(ctx);
      grn_obj *newvalue = grn_ctx_pop(ctx);
      grn_obj *oldvalue = grn_ctx_pop(ctx);
      grn_obj *id = grn_ctx_pop(ctx);

      if (!GRN_BOOL_VALUE(newvalue) && GRN_BOOL_VALUE(oldvalue)) {
        grn_obj *table;
        table = grn_ctx_get(ctx, SETTING_TABLE_NAME, strlen(SETTING_TABLE_NAME));
        if (table) {
          char tokenizer_name[GRN_TABLE_MAX_KEY_SIZE];
          int tokenizer_name_length;
          tokenizer_name_length = grn_table_get_key(ctx, table, GRN_INT32_VALUE(id),
                                                    tokenizer_name,
                                                    GRN_TABLE_MAX_KEY_SIZE);
          tokenizer_name[tokenizer_name_length] = '\0';
          if (tokenizer_name_length) {
            grn_obj *proc;
            proc = grn_ctx_get(ctx, tokenizer_name, tokenizer_name_length);
            grn_obj_close(ctx, proc);
          }
        }
      } else if (GRN_BOOL_VALUE(newvalue) && !GRN_BOOL_VALUE(oldvalue)) {
        grn_plugin_register(ctx, TOKENIZER_PATH_NAME);
      }
    }
  }
  return NULL;
}

static grn_obj *
command_yamecab_delete(grn_ctx *ctx, GNUC_UNUSED int nargs,
                       GNUC_UNUSED grn_obj **args,
                       GNUC_UNUSED grn_user_data *user_data)
{
  if (nargs) {
    grn_obj *obj;
    obj = args[0];
    if (is_table(obj) || is_column(obj)) {
      GNUC_UNUSED grn_obj *flags = grn_ctx_pop(ctx);
      GNUC_UNUSED grn_obj *value = grn_ctx_pop(ctx);
      grn_obj *oldvalue = grn_ctx_pop(ctx);
      GNUC_UNUSED grn_obj *id = grn_ctx_pop(ctx);

      if (GRN_TEXT_LEN(oldvalue)) {
        grn_obj *proc;
        proc = grn_ctx_get(ctx, GRN_TEXT_VALUE(oldvalue), GRN_TEXT_LEN(oldvalue));
        grn_obj_close(ctx, proc);
      }
    }
  }
  return NULL;
}

static grn_obj *
open_or_create_yamecab_table(grn_ctx *ctx, char *table_name, grn_obj *key_type,
                             grn_obj *proc_delete)
{
  grn_obj *table;

  table = grn_ctx_get(ctx, table_name, strlen(table_name));
  if (!table) {
    table = grn_table_create(ctx, table_name, strlen(table_name),
                             NULL,
                             GRN_OBJ_TABLE_HASH_KEY|GRN_OBJ_PERSISTENT,
                             key_type, NULL);
    grn_obj_add_hook(ctx, table, GRN_HOOK_DELETE, 0, proc_delete, 0);
  }
  return table;
}

static grn_obj *
open_or_create_yamecab_column(grn_ctx *ctx, grn_obj *table, char *column_name,
                              grn_obj *value_type, grn_obj *proc_register)
{
  grn_obj *column;

  column = grn_obj_column(ctx, table, column_name, strlen(column_name));
  if (!column) {
    column = grn_column_create(ctx, table, column_name, strlen(column_name),
                               NULL,
                               GRN_OBJ_PERSISTENT|GRN_OBJ_COLUMN_SCALAR,
                               value_type);
    grn_obj_add_hook(ctx, column, GRN_HOOK_SET, 0, proc_register, 0);
  }
  return column;
}

grn_rc
GRN_PLUGIN_INIT(grn_ctx *ctx)
{
  sole_mecab = NULL;
  sole_mecab_mutex = grn_plugin_mutex_open(ctx);
  if (!sole_mecab_mutex) {
    GRN_PLUGIN_ERROR(ctx, GRN_NO_MEMORY_AVAILABLE,
                     "[tokenizer][yamecab] grn_plugin_mutex_open() failed");
    return ctx->rc;
  }

  check_mecab_dictionary_encoding(ctx);

  return ctx->rc;
}

grn_rc
GRN_PLUGIN_REGISTER(grn_ctx *ctx)
{
  grn_obj *table, *proc_register, *proc_delete;
  grn_obj *key_type;

  if (!is_additional_regist) {
    grn_plugin_command_create(ctx, "yamecab_register", -1,
                              command_yamecab_register, 0, NULL);
    grn_plugin_command_create(ctx, "yamecab_delete", -1,
                              command_yamecab_delete, 0, NULL);
    is_additional_regist = GRN_TRUE;
  }

  proc_register = grn_ctx_get(ctx, "yamecab_register", -1);
  proc_delete = grn_ctx_get(ctx, "yamecab_delete", -1);

  key_type = grn_ctx_at(ctx, GRN_DB_SHORT_TEXT);
  table = open_or_create_yamecab_table(ctx,
                                       SETTING_TABLE_NAME,
                                       key_type,
                                       proc_delete);

  if (table) {
    grn_obj *parse_limit_column;
    grn_obj *rfind_punct_offset_column;
    grn_obj *updates_column;
    grn_obj *value_type;
    grn_table_cursor *cur;

    value_type = grn_ctx_at(ctx, GRN_DB_UINT32);
    parse_limit_column =
      open_or_create_yamecab_column(ctx, table, "parse_limit", value_type, NULL);
    rfind_punct_offset_column =
      open_or_create_yamecab_column(ctx, table, "rfind_punct_offset", value_type, NULL);
    value_type = grn_ctx_at(ctx, GRN_DB_BOOL);
    updates_column =
      open_or_create_yamecab_column(ctx, table, "@updates", value_type, proc_register);

    if ((cur = grn_table_cursor_open(ctx, table, NULL, 0, NULL, 0, 0, -1,
                                     GRN_CURSOR_BY_ID))) {
      grn_id id;
      char tokenizer_name[GRN_TABLE_MAX_KEY_SIZE];
      int tokenizer_name_length;
      grn_obj parse_limit;
      grn_obj rfind_punct_offset;

      GRN_INT32_INIT(&parse_limit, 0);
      GRN_INT32_INIT(&rfind_punct_offset, 0);
      while ((id = grn_table_cursor_next(ctx, cur)) != GRN_ID_NIL) {
         tokenizer_name_length = grn_table_get_key(ctx, table, id,
                                                   tokenizer_name,
                                                   GRN_TABLE_MAX_KEY_SIZE);
         tokenizer_name[tokenizer_name_length] = '\0';
         GRN_BULK_REWIND(&parse_limit);
         GRN_BULK_REWIND(&rfind_punct_offset);
         grn_obj_get_value(ctx, parse_limit_column, id, &parse_limit);
         grn_obj_get_value(ctx, rfind_punct_offset_column, id, &rfind_punct_offset);

         if (tokenizer_name_length) {
           grn_expr_var vars[5];
           grn_plugin_expr_var_init(ctx, &vars[0], NULL, -1);
           grn_plugin_expr_var_init(ctx, &vars[1], NULL, -1);
           grn_plugin_expr_var_init(ctx, &vars[2], NULL, -1);
           grn_plugin_expr_var_init(ctx, &vars[3], "parse_limit", -1);
           grn_plugin_expr_var_init(ctx, &vars[4], "rfind_punct_offset", -1);

           if (GRN_INT32_VALUE(&parse_limit)) {
             GRN_INT32_SET(ctx, &vars[3].value, GRN_INT32_VALUE(&parse_limit));
           } else {
             GRN_INT32_SET(ctx, &vars[3].value, DEFAULT_MECAB_PARSE_LIMIT);
           }
           if (GRN_INT32_VALUE(&rfind_punct_offset)) {
             GRN_INT32_SET(ctx, &vars[4].value, GRN_INT32_VALUE(&rfind_punct_offset));
           } else {
             GRN_INT32_SET(ctx, &vars[4].value, DEFAULT_RFIND_PUNCT_OFFSET);
           }

           grn_proc_create(ctx, tokenizer_name, -1,
                           GRN_PROC_TOKENIZER,
                           yamecab_init, yamecab_next, yamecab_fin, 5, vars);
         }
      }
      grn_obj_unlink(ctx, &parse_limit);
      grn_obj_unlink(ctx, &rfind_punct_offset);
      grn_table_cursor_close(ctx, cur);
    }
  }

  return GRN_SUCCESS;
}

grn_rc
GRN_PLUGIN_FIN(grn_ctx *ctx)
{
  if (sole_mecab) {
    mecab_destroy(sole_mecab);
    sole_mecab = NULL;
  }
  if (sole_mecab_mutex) {
    grn_plugin_mutex_close(ctx, sole_mecab_mutex);
    sole_mecab_mutex = NULL;
  }
  is_additional_regist = GRN_FALSE;
  return GRN_SUCCESS;
}
