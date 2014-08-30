AM_CFLAGS =					\
	$(GROONGA_CFLAGS)

AM_LDFLAGS =					\
	-avoid-version				\
	-module					\
	-no-undefined

LIBS =						\
	$(GROONGA_LIBS)

tokenizers_plugins_LTLIBRARIES =
tokenizers_plugins_LTLIBRARIES += yamecab.la 

tokenizers_plugins_SOURCE =			\
	yamecab.c

yamecab_la_CFLAGS = $(AM_CFLAGS) $(MECAB_CPPFLAGS)
yamecab_la_LIBADD = $(LIBS) $(MECAB_LIBS)
yamecab_la_LDFLAGS = $(AM_LDFLAGS) $(MECAB_LDFLAGS)
