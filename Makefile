NPM_BIN := node_modules/.bin/
MOCHA := $(addprefix $(NPM_BIN), mocha)
TEST_FILES := $(addprefix test/, cql2.js cql3.js thrift.js units.js)

test:
	$(MOCHA) --harmony_collections -R spec -t 5000 $(TEST_FILES)

test-cov:
	$(MOCHA) --harmony_collections --require blanket -R travis-cov -t 5000 $(TEST_FILES)

coverage:
	$(MOCHA) --harmony_collections --require blanket -R html-cov -t 5000 $(TEST_FILES) > test/coverage.html

doc:
	rm -rf ./doc && node_modules/JSDoc/jsdoc -p -r ./lib -d ./doc

.PHONY: test test-cov coverage doc
