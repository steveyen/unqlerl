VERSION=$(shell git describe)

EBINS=`find -L "$(PWD)" -name ebin -type d`

all: ebin compile

clean:
	rm -rf ebin *.tar.gz

ebin:
	mkdir -p ebin

compile:
	erl -noinput -eval 'case make:all([]) of up_to_date -> halt(0); error -> halt(1) end.'

test: all
	erl -noinput -pa $(EBINS) -eval "ub_main:test(), halt(0)." && echo "\nOK"

unqlbob-$(VERSION).tar.gz:
	git archive --prefix=unqlbob-$(VERSION)/ --format tar HEAD | gzip -9vc > $@

dist: unqlbob-$(VERSION).tar.gz
