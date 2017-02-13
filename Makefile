
flake:
	flake8 tests


test: flake
	go build .
	@pytest -s -v $(FLAGS) $(FILTER)

v-test: flake
	@pytest -s -vvv $(FLAGS) $(FILTER)
