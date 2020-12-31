/*
  Copyright(C) 2016 Naoya Murakami <naoya@createfield.com>

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

#define GRN_STRING_ENABLE_NORMALIZER_FILTER (0x01<<5)

static mecab_model_t *sole_mecab_model = NULL;
static mecab_t *sole_mecab = NULL;

static grn_plugin_mutex *sole_mecab_mutex = NULL;
static grn_encoding sole_mecab_encoding = GRN_ENC_NONE;

#define DEFAULT_MECAB_PARSE_LIMIT 300000
#define DEFAULT_RFIND_PUNCT_OFFSET 300

typedef struct {
  mecab_model_t *mecab_model;
  mecab_t *mecab;
  mecab_lattice_t *lattice;
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
split_mecab_sparse_node(grn_ctx *ctx, mecab_t *mecab, mecab_lattice_t *lattice, grn_encoding encoding,
                        unsigned int parse_limit, unsigned int rfind_punct_offset,
                        const char *string, unsigned int string_length,
                        unsigned int *parsed_string_length)
{
  const mecab_node_t *node;

  if (string_length == 0) {
    return NULL;
  }
  if (string_length < parse_limit) {
    mecab_lattice_set_sentence2(lattice, string, string_length);
    mecab_parse_lattice(mecab, lattice);
    node = mecab_lattice_get_bos_node(lattice);

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

    mecab_lattice_set_sentence2(lattice, string, splitted_string_length);
    mecab_parse_lattice(mecab, lattice);
    node = mecab_lattice_get_bos_node(lattice);

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
  const char *value;
  uint32_t value_size;

  query = grn_tokenizer_query_open(ctx, nargs, args, normalizer_flags);
  if (!query) {
    return NULL;
  }
  if (query->length == 0) {
    ctx->errbuf[0] = '\0';
    ctx->rc = GRN_SUCCESS;
  }


  if (!sole_mecab) {
    grn_plugin_mutex_lock(ctx, sole_mecab_mutex);
    if (!sole_mecab) {
      const char v[] = "-Owakati";
      const char *opt = &v[0];

      sole_mecab_model = mecab_model_new(1, (char **)&opt);
      sole_mecab = mecab_model_new_tagger(sole_mecab_model);

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
  tokenizer->mecab_model = sole_mecab_model;
  tokenizer->mecab = sole_mecab;
  tokenizer->lattice = mecab_model_new_lattice(sole_mecab_model);
  tokenizer->query = query;

  normalized_query = query->normalized_query;
  grn_string_get_normalized(ctx,
                            normalized_query,
                            &normalized_string,
                            &normalized_string_length,
                            NULL);

  grn_config_get(ctx, "tokenizer-yamecab.parse_limit",
                 strlen("tokenizer-yamecab.parse_limit"),
                 &value, &value_size);
  if (value_size) {
    tokenizer->parse_limit = atoi(value);
  } else {
    tokenizer->parse_limit = DEFAULT_MECAB_PARSE_LIMIT;
  }
  grn_config_get(ctx, "tokenizer-yamecab.rfind_punct_offset",
                 strlen("tokenizer-yamecab.rfind_punct_offset"),
                 &value, &value_size);
  if (value_size) {
    tokenizer->rfind_punct_offset = atoi(value);
  } else {
    tokenizer->rfind_punct_offset = DEFAULT_RFIND_PUNCT_OFFSET;
  }

  tokenizer->node = NULL;
  tokenizer->rest_length = 0;
  tokenizer->rest_string = NULL;
  if (normalized_string_length > 0) {
#define MECAB_PARSE_MIN 4096
    unsigned int parsed_string_length;
    grn_bool is_success = GRN_FALSE;
    while (!is_success) {
      tokenizer->node = split_mecab_sparse_node(ctx, tokenizer->mecab, tokenizer->lattice,
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

  if (!tokenizer || !tokenizer->node) {
    status = GRN_TOKENIZER_LAST;
    grn_tokenizer_token_push(ctx, &(tokenizer->token),
                             NULL, 0,
                             status);
    return NULL;
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
        status |= GRN_TOKEN_SKIP_WITH_POSITION;
      }
    } else {
      status = GRN_TOKENIZER_LAST;
    }
  }

  /* should be customizable */
  if (!(!strncmp(tokenizer->node->feature, "名詞", 6) ||
        !strncmp(tokenizer->node->feature, "動詞", 6) ||
        !strncmp(tokenizer->node->feature, "形容詞", 9) ||
        !strncmp(tokenizer->node->feature, "連体詞", 9) ||
        tokenizer->node->stat == MECAB_UNK_NODE)) {
    status |= GRN_TOKEN_SKIP;
  }

  grn_tokenizer_token_push(ctx, &(tokenizer->token),
                           tokenizer->node->surface, tokenizer->node->length,
                           status);

  if (!tokenizer->node->next && tokenizer->rest_length) {
    {
#define MECAB_PARSE_MIN 4096
      unsigned int parsed_string_length;
      grn_bool is_success = GRN_FALSE;
      while (!is_success) {
        tokenizer->node = split_mecab_sparse_node(ctx, tokenizer->mecab, tokenizer->lattice,
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
  }
  if (tokenizer->node && tokenizer->node->next) {
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
  mecab_lattice_destroy(tokenizer->lattice);
  grn_tokenizer_token_fin(ctx, &(tokenizer->token));
  grn_tokenizer_query_close(ctx, tokenizer->query);
  GRN_PLUGIN_FREE(ctx, tokenizer);
  return NULL;
}

static void
check_mecab_dictionary_encoding(GNUC_UNUSED grn_ctx *ctx)
{
#ifdef HAVE_MECAB_DICTIONARY_INFO_T
  mecab_model_t *model;
  mecab_t *mecab;
  const char v[] = "-Owakati";
  const char *opt = &v[0];

  model = mecab_model_new(1, (char **)&opt);

  mecab = mecab_model_new_tagger(model);

  if (mecab) {
    grn_encoding encoding;
    int have_same_encoding_dictionary = 0;

    encoding = GRN_CTX_GET_ENCODING(ctx);
    have_same_encoding_dictionary = encoding == get_mecab_encoding(mecab);
    mecab_destroy(mecab);
    mecab_model_destory(model);

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
  grn_rc rc;

  rc = grn_tokenizer_register(ctx, "TokenYaMecab", -1,
                              yamecab_init, yamecab_next, yamecab_fin);
  return rc;
}

grn_rc
GRN_PLUGIN_FIN(grn_ctx *ctx)
{
  if (sole_mecab) {
    mecab_destroy(sole_mecab);
    mecab_model_destroy(sole_mecab_model);

    sole_mecab = NULL;
    sole_mecab_model = NULL;
  }
  if (sole_mecab_mutex) {
    grn_plugin_mutex_close(ctx, sole_mecab_mutex);
    sole_mecab_mutex = NULL;
  }
  return GRN_SUCCESS;
}
