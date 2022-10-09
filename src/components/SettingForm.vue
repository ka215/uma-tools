<script setup lang="ts">
import InputItem from "./WelcomeItem.vue";
import DocumentationIcon from "./icons/IconDocumentation.vue";
/*
import ToolingIcon from "./icons/IconTooling.vue";
import EcosystemIcon from "./icons/IconEcosystem.vue";
import CommunityIcon from "./icons/IconCommunity.vue";
import SupportIcon from "./icons/IconSupport.vue";
*/
</script>

<template>
  <InputItem>
    <template #icon>
      <DocumentationIcon />
    </template>
    <template #heading>Documentation</template>

    Vue’s
    <a href="https://vuejs.org/" target="_blank" rel="noopener"
      >official documentation</a
    >
    provides you with all information you need to get started.
  </InputItem>
</template>
