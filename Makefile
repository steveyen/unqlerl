VERSION=$(shell git describe)

all: ebin compile

clean:
	rm -rf ebin *.tar.gz

ebin:
	mkdir -p ebin

compile:
	erl -pa build -noinput +B -eval 'case make:all([]) of up_to_date -> halt(0); error -> halt(1) end.'

unqlbob-$(VERSION).tar.gz:
	git archive --prefix=unqlbob-$(VERSION)/ --format tar HEAD | gzip -9vc > $@

dist: unqlbob-$(VERSION).tar.gz
