# Define tofu_dir if not passed in externally
env ?= dev
target ?= advertising

# Using a recipe macro to improve DRY
# @bash -c 'source .env && source live/$(subject)/$(env)/$(country)/.env.secrets && tofu -chdir=live/$(subject)/$(env)/$(country) $(1)'
define TOFU_CMD
	@bash -c 'source ./bin/tofu_wrapper.sh && tofu_cmd $(env) $(target) $(1)'
endef

.PHONY: setup tofu-init tofu-plan tofu-apply tofu-fmt tofu-state-pull tofu-state-list tofu-destroy

setup:
	@bash ./bin/setup.sh

tofu-init:
	$(call TOFU_CMD,init)

tofu-plan:
	$(call TOFU_CMD,plan)

tofu-apply:
	$(call TOFU_CMD,apply)

tofu-fmt:
	$(call TOFU_CMD,fmt)

tofu-state-pull:
	$(call TOFU_CMD,"state pull")

tofu-state-list:
	$(call TOFU_CMD,"state list")

tofu-destroy:
	$(call TOFU_CMD,destroy)
