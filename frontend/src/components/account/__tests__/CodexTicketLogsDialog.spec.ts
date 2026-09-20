import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import CodexTicketLogsDialog from '../CodexTicketLogsDialog.vue'

const { getLogs } = vi.hoisted(() => ({ getLogs: vi.fn() }))
vi.mock('@/api/admin/accounts', () => ({ getCodexTicketLogs: getLogs }))
vi.mock('vue-i18n', () => ({ useI18n: () => ({ t: (key: string, params?: unknown) => `${key}${params ? JSON.stringify(params) : ''}` }) }))
const result = (reason = 'harvested') => ({ model: 'gpt-6-astra', limit: 200, target_length: 292,
  status: { probe_attempts: 3, token_invalid: true },
  entries: [
    { id: 1, time: '2026-09-20T00:00:00Z', attempt: 3, event: 'started', reason: 'request_started', ticket_length: 0, duration_ms: 0 },
    { id: 2, time: '2026-09-20T00:00:01Z', attempt: 3, event: 'received', reason: 'valid_ticket', http_status: 200, ticket_length: 292, duration_ms: 200 },
    { id: 3, time: '2026-09-20T00:00:01Z', attempt: 3, event: 'saved', reason, ticket_length: 0, duration_ms: 0 },
  ] })
function mountDialog(show = true) {
  return mount(CodexTicketLogsDialog, {
    props: { show, account: { id: 1, name: 'Test' }, model: 'gpt-6-astra' },
    global: { stubs: { BaseDialog: { template: '<div><slot /><slot name="footer" /></div>' } } }
  })
}
describe('CodexTicketLogsDialog', () => {
  beforeEach(() => { vi.useFakeTimers(); getLogs.mockReset(); getLogs.mockResolvedValue(result()) })
  afterEach(() => vi.useRealTimers())
  it('renders diagnostics and counts saved tickets only once', async () => {
    const w = mountDialog(); await flushPromises()
    expect(w.text()).toContain('codexTurnTicketTokenInvalid')
    expect(w.text()).toContain('292 / 292')
    expect(w.text()).toContain('200 ms')
    expect(w.get('[data-test="log-summary"]').text()).toContain('"requests":1,"saved":1,"misses":0')
    expect(getLogs).toHaveBeenCalledWith(1, 'gpt-6-astra', expect.any(AbortSignal))
    w.unmount()
  })
  it('polls only while visible, cancels and does not leak timers', async () => {
    const w = mountDialog(false); await flushPromises(); expect(getLogs).not.toHaveBeenCalled()
    await w.setProps({ show: true }); await flushPromises()
    await vi.advanceTimersByTimeAsync(5000); expect(getLogs).toHaveBeenCalledTimes(2)
    const signal = getLogs.mock.calls[1][2]
    await w.setProps({ show: false }); expect(signal.aborted).toBe(true)
    await vi.advanceTimersByTimeAsync(15000); expect(getLogs).toHaveBeenCalledTimes(2)
    w.unmount(); expect(vi.getTimerCount()).toBe(0)
  })
  it('does not let old account results overwrite a new selection', async () => {
    let resolveOld!: (value: unknown) => void
    getLogs.mockImplementationOnce(() => new Promise(resolve => { resolveOld = resolve }))
    const w = mountDialog(); await flushPromises()
    await w.setProps({ account: { id: 2, name: 'Other' } }); await flushPromises()
    resolveOld(result('timeout')); await flushPromises()
    expect(w.text()).toContain('reasons.harvested'); expect(w.text()).not.toContain('reasons.timeout')
    w.unmount()
  })
  it('keeps the last snapshot on error and never displays raw error secrets', async () => {
    const w = mountDialog(); await flushPromises()
    getLogs.mockRejectedValueOnce(new Error('secret-key proxy-password'))
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect(w.find('[role="alert"]').exists()).toBe(true)
    expect(w.text()).toContain('reasons.harvested')
    expect(w.text()).not.toContain('secret-key')
    w.unmount()
  })
})
