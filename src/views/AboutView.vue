<script setup lang="ts">
  import SettingForm from "../components/SettingForm.vue";
</script>

<template>
  <main class="settings">
    <h1>Uma All Crown</h1>
    <SettingForm />
  </main>
</template>

<style>
@media (min-width: 1024px) {
  .settings {
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    align-items: center;
  }
}
</style>
