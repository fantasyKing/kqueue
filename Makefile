dist:
	./node_modules/.bin/babel src --out-dir dist

start:
	./node_modules/.bin/babel-node example/index.js

transform_example:
	./node_modules/.bin/babel example --out-dir out

example: dist transform_example

.PHONY: test build dist start example
