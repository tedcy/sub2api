<template>
  <BaseDialog :show="show" :title="t('admin.accounts.openai.ticketLogs.title')" width="extra-wide" @close="emit('close')">
    <div class="space-y-3">
      <p class="break-all text-sm">{{ account.name }} #{{ account.id }} · {{ model }}</p>
      <p class="text-xs text-gray-500">{{ t('admin.accounts.openai.ticketLogs.retention', { count: data?.limit ?? 200 }) }}</p>
      <p v-if="data?.status" class="text-sm">
        {{ t('admin.accounts.openai.codexTurnTicketAttempts', { count: data.status.probe_attempts ?? 0 }) }}
        <span v-if="data.status.token_invalid"> · {{ t('admin.accounts.openai.codexTurnTicketTokenInvalid') }}</span>
        <span v-else-if="data.status.harvest_paused"> · {{ t('admin.accounts.openai.codexTurnTicketQuotaPaused') }}</span>
        <span v-else-if="data.status.rate_limited"> · {{ t('admin.accounts.openai.codexTurnTicketRateLimited') }}</span>
      </p>
      <p class="text-sm" data-test="log-summary">{{ t('admin.accounts.openai.ticketLogs.summary', summary) }}</p>
      <p v-if="failed" role="alert" class="text-sm text-red-600">{{ t('admin.accounts.openai.ticketLogs.failed') }}</p>
      <p v-if="loading" role="status">{{ t('common.loading') }}</p>
      <p v-else-if="!data?.entries.length && !failed" class="text-sm text-gray-500">{{ t('admin.accounts.openai.ticketLogs.empty') }}</p>
      <div v-if="data?.entries.length" class="max-h-[50vh] overflow-auto">
        <table class="w-full text-left text-xs">
          <thead><tr>
            <th class="p-2">{{ t('admin.accounts.openai.ticketLogs.time') }}</th>
            <th class="p-2">{{ t('admin.accounts.openai.ticketLogs.attempt') }}</th>
            <th class="p-2">{{ t('admin.accounts.openai.ticketLogs.result') }}</th>
            <th class="p-2">HTTP</th>
            <th class="p-2">{{ t('admin.accounts.openai.ticketLogs.length') }}</th>
            <th class="p-2">{{ t('admin.accounts.openai.ticketLogs.duration') }}</th>
          </tr></thead>
          <tbody><tr v-for="entry in [...data.entries].reverse()" :key="entry.id" class="border-t border-gray-100 dark:border-dark-700">
            <td class="whitespace-nowrap p-2">{{ new Date(entry.time).toLocaleString() }}</td>
            <td class="p-2">{{ entry.attempt || '—' }}</td>
            <td class="p-2">{{ reasonLabel(entry.reason) }}</td>
            <td class="p-2">{{ entry.http_status || '—' }}</td>
            <td class="p-2">{{ entry.http_status ? `${entry.ticket_length} / ${data.target_length}` : '—' }}</td>
            <td class="p-2">{{ entry.event === 'miss' || entry.event === 'received' || entry.event === 'error' ? `${entry.duration_ms} ms` : '—' }}</td>
          </tr></tbody>
        </table>
      </div>
    </div>
    <template #footer><div class="flex justify-end gap-2">
      <button class="btn btn-secondary" :disabled="loading" @click="refresh">{{ t('common.refresh') }}</button>
      <button class="btn btn-primary" @click="emit('close')">{{ t('common.close') }}</button>
    </div></template>
  </BaseDialog>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import BaseDialog from '@/components/common/BaseDialog.vue'
import { getCodexTicketLogs, type CodexTicketLogsResponse } from '@/api/admin/accounts'
import type { Account } from '@/types'

const props = defineProps<{ show: boolean; account: Pick<Account, 'id' | 'name'>; model: string }>()
const emit = defineEmits<{ close: [] }>()
const { t } = useI18n()
const data = ref<CodexTicketLogsResponse | null>(null)
const failed = ref(false)
const loading = ref(false)
let controller: AbortController | undefined
let timer: ReturnType<typeof setTimeout> | undefined
let generation = 0
const summary = computed(() => {
  const entries = data.value?.entries ?? []
  return {
    requests: entries.filter(e => e.event === 'started').length,
    saved: entries.filter(e => e.event === 'saved').length,
    misses: entries.filter(e => e.event === 'miss' || e.event === 'error' || e.event === 'discarded').length
  }
})
const reasons = new Set(['request_started', 'token_error', 'token_invalid', 'quota_exhausted', 'http_error', 'missing_state', 'length_mismatch', 'invalid_state', 'valid_ticket', 'harvested', 'save_rejected', 'timeout', 'canceled', 'request_error'])
function reasonLabel(reason: string) {
  return t(`admin.accounts.openai.ticketLogs.reasons.${reasons.has(reason) ? reason : 'request_error'}`)
}
function stop() {
  generation++
  controller?.abort()
  clearTimeout(timer)
}
async function refresh() {
  stop()
  if (!props.show) return
  const current = generation
  controller = new AbortController()
  loading.value = true
  try {
    const result = await getCodexTicketLogs(props.account.id, props.model, controller.signal)
    if (current !== generation) return
    data.value = result
    failed.value = false
  } catch {
    if (current === generation) failed.value = true
  } finally {
    if (current === generation) {
      loading.value = false
      if (props.show) timer = setTimeout(refresh, 5000)
    }
  }
}
watch(() => [props.show, props.account.id, props.model] as const, () => {
  stop()
  data.value = null
  failed.value = false
  loading.value = false
  if (props.show) void refresh()
}, { immediate: true })
onBeforeUnmount(stop)
</script>
