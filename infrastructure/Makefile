env    ?= dev
target ?= advertising

define TG_CMD
	@bash -c 'source ./bin/terragrunt_wrapper.sh && tg_cmd $(target) $(env) $(1)'
endef

.PHONY: setup tg-init tg-plan tg-apply tg-fmt tg-state-pull tg-state-list tg-destroy

setup:
	@$$SHELL ./bin/setup.sh

tg-init:
	$(call TG_CMD,init)

tg-plan:
	$(call TG_CMD,plan)

tg-apply:
	$(call TG_CMD,apply)

tg-fmt:
	$(call TG_CMD,hclfmt)

tg-state-pull:
	$(call TG_CMD,"state pull")

tg-state-list:
	$(call TG_CMD,"state list")

tg-destroy:
	$(call TG_CMD,destroy)
