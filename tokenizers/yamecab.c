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

static mecab_t *sole_mecab = NULL;
static grn_plugin_mutex *sole_mecab_mutex = NULL;
static grn_encoding sole_mecab_encoding = GRN_ENC_NONE;

typedef struct {
  mecab_t *mecab;
  grn_obj buf;
  const char *next;
  const char *end;
  grn_tokenizer_query *query;
  grn_tokenizer_token token;
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
rfind_punct(grn_ctx *ctx, grn_yamecab_tokenizer *tokenizer,
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
                                           end - start, tokenizer->query->encoding);
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
rfind_lastbyte(GNUC_UNUSED grn_ctx *ctx, grn_yamecab_tokenizer *tokenizer,
               const char *string, int offset)
{
  switch (tokenizer->query->encoding) {
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

static grn_bool
mecab_sparse(grn_ctx *ctx, grn_yamecab_tokenizer *tokenizer,
             const char *string, unsigned int string_length)
{
  const char *parsed_string;
  char *parsed_string_end;
  parsed_string = mecab_sparse_tostr2(tokenizer->mecab,
                                      string,
                                      string_length);
  if (!parsed_string) {
    GRN_PLUGIN_ERROR(ctx, GRN_TOKENIZER_ERROR,
                     "[tokenizer][yamecab] "
                     "mecab_sparse_tostr() failed len=%d err=%s",
                     string_length,
                     mecab_strerror(tokenizer->mecab));
    return GRN_FALSE;
  } else {
    unsigned int parsed_string_length;
    parsed_string_length = strlen(parsed_string);
    parsed_string_end = (char *)parsed_string + parsed_string_length - 2;
    while (parsed_string_end > parsed_string &&
           isspace(*parsed_string_end)) {
      *parsed_string_end = '\0';
      parsed_string_end--;
    }
    parsed_string_end += 1;
    GRN_TEXT_PUTS(ctx, &tokenizer->buf, parsed_string);
  }
  return GRN_TRUE;
}

static grn_obj *
yamecab_init(grn_ctx *ctx, int nargs, grn_obj **args, grn_user_data *user_data)
{
  grn_yamecab_tokenizer *tokenizer;
  unsigned int normalizer_flags = 0;
  grn_tokenizer_query *query;
  grn_obj *normalized_query;
  const char *normalized_string;
  unsigned int normalized_string_length;
  unsigned int parse_limit = 0;
  unsigned int rfind_punct_offset = 0;
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
    parse_limit = GRN_UINT32_VALUE(var);
  }
  var = grn_plugin_proc_get_var(ctx, user_data, "rfind_punct_offset", -1);
  if (GRN_TEXT_LEN(var) != 0) {
    rfind_punct_offset = GRN_UINT32_VALUE(var);
  }

  if (query->have_tokenized_delimiter) {
    tokenizer->next = normalized_string;
    tokenizer->end = tokenizer->next + normalized_string_length;
  } else if (normalized_string_length == 0) {
    tokenizer->next = "";
    tokenizer->end = tokenizer->next;
  } else {
    grn_plugin_mutex_lock(ctx, sole_mecab_mutex);

    GRN_TEXT_INIT(&(tokenizer->buf), 0);
    grn_bool rc = GRN_FALSE;

    if (normalized_string_length < parse_limit) {
      rc = mecab_sparse(ctx, tokenizer, normalized_string, normalized_string_length);
    } else {
      int splitted_string_start = 0;
      int splitted_string_end = parse_limit;
      unsigned int splitted_string_length;
      int punct_position = splitted_string_end;

      while (splitted_string_end < (int)normalized_string_length) {
        splitted_string_end = rfind_lastbyte(ctx, tokenizer,
                                             normalized_string,
                                             splitted_string_end);
        if (splitted_string_end == splitted_string_start) {
          splitted_string_end = splitted_string_start + parse_limit;
        } 

        punct_position = rfind_punct(ctx, tokenizer,
                                     normalized_string,
                                     splitted_string_start,
                                     splitted_string_end - rfind_punct_offset,
                                     splitted_string_end);

        splitted_string_length = punct_position - splitted_string_start;
        rc = mecab_sparse(ctx, tokenizer,
                          normalized_string + splitted_string_start,
                          splitted_string_length);
        if (!rc) {
          break;
        }
        GRN_TEXT_PUTS(ctx, &tokenizer->buf, " ");
        splitted_string_start = punct_position;
        splitted_string_end = splitted_string_start + parse_limit;
        punct_position = splitted_string_end;
      }
      splitted_string_length = normalized_string_length - splitted_string_start;
      if (rc && splitted_string_length) {
        rc = mecab_sparse(ctx, tokenizer,
                          normalized_string + splitted_string_start,
                          splitted_string_length);
      }
    }

    grn_plugin_mutex_unlock(ctx, sole_mecab_mutex);
    if (!rc) {
      grn_tokenizer_query_close(ctx, tokenizer->query);
      GRN_PLUGIN_FREE(ctx, tokenizer);
      return NULL;
    }
    tokenizer->next = GRN_TEXT_VALUE(&tokenizer->buf);
    tokenizer->end = tokenizer->next + GRN_TEXT_LEN(&tokenizer->buf);
  }
  user_data->ptr = tokenizer;
  grn_tokenizer_token_init(ctx, &(tokenizer->token));

  return NULL;
}

static grn_obj *
yamecab_next(grn_ctx *ctx, GNUC_UNUSED int nargs, GNUC_UNUSED grn_obj **args,
             grn_user_data *user_data)
{
  grn_yamecab_tokenizer *tokenizer = user_data->ptr;
  grn_encoding encoding = tokenizer->query->encoding;

  if (tokenizer->query->have_tokenized_delimiter) {
    tokenizer->next =
      grn_tokenizer_tokenized_delimiter_next(ctx,
                                             &(tokenizer->token),
                                             tokenizer->next,
                                             tokenizer->end - tokenizer->next,
                                             encoding);
  } else {
    const char *token_top = tokenizer->next;
    const char *token_tail;
    const char *string_end = tokenizer->end;

    unsigned int char_length;
    unsigned int rest_length = string_end - token_top;
    grn_tokenizer_status status;

    for (token_tail = token_top; token_tail < string_end; token_tail += char_length) {
      if (!(char_length = grn_plugin_charlen(ctx, token_tail,rest_length, encoding))) {
        tokenizer->next = string_end;
        break;
      }
      if (grn_plugin_isspace(ctx, token_tail, rest_length, encoding)) {
        const char *token_next = token_tail;
        while ((char_length = grn_plugin_isspace(ctx, token_next, rest_length, encoding))) {
          token_next += char_length;
        }
        tokenizer->next = token_next;
        break;
      }
    }

    if (token_tail == string_end) {
      status = GRN_TOKENIZER_LAST;
    } else {
      status = GRN_TOKENIZER_CONTINUE;
    }
    grn_tokenizer_token_push(ctx, &(tokenizer->token),
                             token_top, token_tail - token_top, status);
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
  grn_obj_unlink(ctx, &(tokenizer->buf));
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

static grn_obj *
command_yamecab_register(grn_ctx *ctx, GNUC_UNUSED int nargs,
                         GNUC_UNUSED grn_obj **args, grn_user_data *user_data)
{
#define DEFAULT_MECAB_PARSE_LIMIT 2100000
#define DEFAULT_RFIND_PUNCT_OFFSET 300

  int parse_limit = DEFAULT_MECAB_PARSE_LIMIT;
  int rfind_punct_offset = DEFAULT_RFIND_PUNCT_OFFSET;

  grn_obj tokenizer_name;
  grn_obj *var;
  grn_expr_var vars[5];

  grn_plugin_expr_var_init(ctx, &vars[0], NULL, -1);
  grn_plugin_expr_var_init(ctx, &vars[1], NULL, -1);
  grn_plugin_expr_var_init(ctx, &vars[2], NULL, -1);
  grn_plugin_expr_var_init(ctx, &vars[3], "parse_limit", -1);
  grn_plugin_expr_var_init(ctx, &vars[4], "rfind_punct_offset", -1);

  GRN_INT32_SET(ctx, &vars[3].value, parse_limit);
  GRN_INT32_SET(ctx, &vars[4].value, rfind_punct_offset);

  GRN_TEXT_INIT(&tokenizer_name, 0);
  GRN_BULK_REWIND(&tokenizer_name);
  GRN_TEXT_PUTS(ctx, &tokenizer_name, "TokenYaMecab");

  var = grn_plugin_proc_get_var(ctx, user_data, "parse_limit", -1);
  if (GRN_TEXT_LEN(var) != 0) {
    parse_limit = atoi(GRN_TEXT_VALUE(var));
    GRN_INT32_SET(ctx, &vars[3].value, parse_limit);
  }
  var = grn_plugin_proc_get_var(ctx, user_data, "rfind_punct_offset", -1);
  if (GRN_TEXT_LEN(var) != 0) {
    parse_limit = atoi(GRN_TEXT_VALUE(var));
    GRN_INT32_SET(ctx, &vars[4].value, rfind_punct_offset);
  }

  GRN_TEXT_PUTC(ctx, &tokenizer_name, '\0');
  grn_proc_create(ctx, GRN_TEXT_VALUE(&tokenizer_name), -1,
                  GRN_PROC_TOKENIZER,
                  yamecab_init, yamecab_next, yamecab_fin, 5, vars);

  grn_ctx_output_cstr(ctx, GRN_TEXT_VALUE(&tokenizer_name));
  grn_obj_unlink(ctx, &tokenizer_name);

  return NULL;

#undef DEFAULT_MECAB_PARSE_LIMIT
#undef DEFAULT_RFIND_PUNCT_OFFSET
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
  grn_expr_var vars[2];
 
  grn_plugin_expr_var_init(ctx, &vars[0], "parse_limit", -1);
  grn_plugin_expr_var_init(ctx, &vars[1], "rfind_punct_offset", -1); 

  grn_plugin_command_create(ctx, "yamecab_register", -1,
                            command_yamecab_register, 2, vars);

  return ctx->rc;
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

  return GRN_SUCCESS;
}
