
FF_ROOT	= $(HOME)/fastflow
FF_REPO	= https://github.com/fastflow/fastflow

all: fastflow
	mkdir -p ./bin
	$(MAKE) -C src

taxi_test: fastflow
	mkdir -p ./bin
	$(MAKE) -C src

fastflow:
	@if [ ! -d $(FF_ROOT) ] ;\
	then \
	  echo "FastFlow does not exist, fetching"; \
	  git clone $(FF_REPO) $(FF_ROOT); \
fi

clean:
	$(MAKE) clean -C src

.DEFAULT_GOAL := all
.PHONY: all taxi_test fastflow clean
