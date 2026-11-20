<template>
  <section class="page recovery-page" data-testid="recovery-page">
    <header class="page-head">
      <div>
        <h2>去重窗口 · 历史交付恢复</h2>
        <p class="page-desc">
          从交付记录文件按来源导入已下发事件的位置记录，归并生成窗口恢复数据集；
          事件身份由源实例、来源表、去重键与下发位点联合刻画，西侧回执不能抵扣同键的东侧事件。
        </p>
      </div>
      <div class="page-actions">
        <RouterLink class="btn" to="/dedup_window">返回去重窗口</RouterLink>
        <button class="btn" type="button" @click="reloadAll">刷新数据</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">恢复清单总数</span>
        <strong class="stat-value">{{ stats.恢复清单总数 ?? '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已确认 / 待核实回执</span>
        <strong class="stat-value">{{ stats.已确认回执数 ?? 0 }} / {{ stats.待核实记录数 ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">累计拦截 / 放行重放</span>
        <strong class="stat-value">
          {{ stats.累计拦截重放数 ?? 0 }} / {{ stats.累计放行重放数 ?? 0 }}
        </strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">中断会话 / 暂存待核对</span>
        <strong class="stat-value">
          {{ stats.中断会话数 ?? 0 }} / {{ stats.暂存待核对条数 ?? 0 }}
        </strong>
      </article>
    </div>

    <p v-if="message" :class="['flash', messageOk ? 'flash-ok' : 'flash-err']" data-testid="flash">
      {{ message }}
    </p>

    <!-- 导入面板 -->
    <article class="panel">
      <h3>① 导入交付记录文件</h3>
      <p class="hint">
        同一文件按内容指纹去重，重复导入只生效一次；设置「首批核对条数」可模拟上传中断，
        未核对条目进入暂存区，随后在导入会话中续导。
      </p>
      <div class="import-grid">
        <label class="create-field">
          <span>文件名称</span>
          <input v-model="importForm.file_name" data-testid="import-file-name" placeholder="delivery-orders-west-xxxx.json" />
        </label>
        <label class="create-field">
          <span>首批核对条数（留空=一次性核对）</span>
          <input
            v-model="importForm.batch_size"
            data-testid="import-batch-size"
            type="number"
            min="0"
            placeholder="例如 1，模拟上传中断"
          />
        </label>
        <label class="create-field file-pick">
          <span>选择本地交付记录文件（JSON）</span>
          <input type="file" accept=".json,application/json" data-testid="import-file-pick" @change="onPickFile" />
        </label>
      </div>
      <label class="create-field">
        <span>交付记录内容（JSON：含 source_instance 与 records，每条带来源表/去重键/日志文件/日志位点/回执确认）</span>
        <textarea
          v-model="importForm.content"
          class="json-box"
          data-testid="import-content"
          rows="10"
          placeholder='{"file_name":"...","source_instance":"orders-west","records":[...]}'
        ></textarea>
      </label>
      <div class="page-actions">
        <button class="btn ghost" type="button" @click="fillScenarioSample">填入西侧交付场景样例</button>
        <button class="btn primary" type="button" data-testid="submit-import" :disabled="busy" @click="submitImport">
          {{ busy ? '导入中…' : '导入并核对来源' }}
        </button>
      </div>
    </article>

    <!-- 导入会话 -->
    <article class="panel">
      <h3>② 导入会话与断点续导</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>ID</th><th>文件名称</th><th>文件指纹</th><th>状态</th>
            <th>核对进度</th><th>新增/归并/重复</th><th>最近更新</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="session in sessions" :key="session.id" :data-testid="`session-${session.id}`">
            <td>{{ session.id }}</td>
            <td>{{ session.文件名称 }}</td>
            <td class="mono">{{ session.文件指纹 }}</td>
            <td>
              <span :class="['tag', sessionStatusClass(session.状态)]">{{ session.状态 }}</span>
            </td>
            <td>{{ session.已核对条数 }} / {{ session.条目总数 }}</td>
            <td>{{ session.新增条数 }} / {{ session.归并更新条数 }} / {{ session.重复条数 }}</td>
            <td>{{ formatTime(session.最近更新时间) }}</td>
            <td class="row-actions">
              <button
                v-if="session.状态 === '中断'"
                class="link"
                type="button"
                :data-testid="`resume-${session.id}`"
                @click="submitResume(session.文件指纹, null)"
              >
                继续核对
              </button>
              <span v-else class="muted-text">已生效，无需重复导入</span>
            </td>
          </tr>
          <tr v-if="!sessions.length">
            <td colspan="8" class="empty-state">暂无导入会话</td>
          </tr>
        </tbody>
      </table>
      <div v-if="pending.length" class="pending-box" data-testid="pending-box">
        <strong>暂存区待核对来源（{{ pending.length }} 条）：</strong>
        <ul>
          <li v-for="(item, idx) in pending" :key="`${item.文件指纹}-${item.序号}`">
            <span class="mono">{{ item.条目.source_instance }}</span>
            / {{ item.条目.source_table }} # {{ item.条目.dedup_key }}
            @ {{ item.条目.binlog_file }}:{{ item.条目.binlog_pos }}
            <em :class="item.条目.receipt_confirmed ? 'receipt-yes' : 'receipt-no'">
              {{ item.条目.receipt_confirmed ? '带已确认回执' : '无回执（未核实）' }}
            </em>
            <button
              v-if="idx === 0"
              class="link"
              type="button"
              @click="submitResume(item.文件指纹, 1)"
            >
              逐条续导
            </button>
          </li>
        </ul>
      </div>
    </article>

    <!-- 重放核对 -->
    <article class="panel">
      <h3>③ 消费端重放核对（关联恢复数据集）</h3>
      <p class="hint">
        仅同源实例、同来源表、同去重键且带已确认回执的记录会拦截下发；
        跨源同键回执只列出、不抵扣；处置结果回写到恢复清单。
      </p>
      <div class="replay-grid">
        <label class="create-field">
          <span>源实例</span>
          <input v-model="replayForm.source_instance" data-testid="replay-instance" placeholder="orders-west / orders-east" />
        </label>
        <label class="create-field">
          <span>来源表 source_table</span>
          <input v-model="replayForm.source_table" data-testid="replay-table" placeholder="orders" />
        </label>
        <label class="create-field">
          <span>去重键</span>
          <input v-model="replayForm.dedup_key" data-testid="replay-key" placeholder="ORD-8899" />
        </label>
        <label class="create-field">
          <span>日志文件</span>
          <input v-model="replayForm.binlog_file" data-testid="replay-binlog" placeholder="mysql-bin.000201" />
        </label>
        <label class="create-field">
          <span>日志位点</span>
          <input v-model="replayForm.binlog_pos" data-testid="replay-pos" type="number" placeholder="8821" />
        </label>
        <label class="create-field">
          <span>事件ID（可选）</span>
          <input v-model="replayForm.event_id" data-testid="replay-event-id" />
        </label>
      </div>
      <div class="page-actions">
        <button class="btn ghost" type="button" @click="fillWestReplay">填入西侧同键坐标</button>
        <button class="btn ghost" type="button" @click="fillEastReplay">填入东侧同键坐标</button>
        <button class="btn primary" type="button" data-testid="submit-replay" :disabled="busy" @click="submitReplay">
          核对下发
        </button>
      </div>

      <div v-if="replayResult" class="replay-result" data-testid="replay-result">
        <div class="decision-head">
          <span :class="['decision', replayResult.下发决定 === '拦截' ? 'decision-block' : 'decision-pass']">
            {{ replayResult.下发决定 }}
          </span>
          <strong>{{ replayResult.决定原因 }}</strong>
        </div>
        <dl class="coord-grid">
          <dt>事件身份</dt><dd class="mono">{{ replayResult.事件身份 }}</dd>
          <dt>当前窗口版本</dt><dd class="mono">{{ replayResult.窗口版本比对.当前窗口版本 }}</dd>
          <dt>历史窗口版本</dt><dd class="mono">{{ replayResult.窗口版本比对.历史窗口版本 ?? '—' }}</dd>
          <dt>版本比对结论</dt><dd>{{ replayResult.窗口版本比对.比对结论 }}</dd>
          <dt>命中清单ID</dt><dd>{{ replayResult.命中清单ID ?? '无' }}</dd>
          <dt>处置回写</dt>
          <dd>
            <template v-if="replayResult.回写结果.已回写">
              已回写清单 #{{ replayResult.回写结果.清单ID }} → {{ replayResult.回写结果.处置结果 }}
            </template>
            <template v-else>{{ replayResult.回写结果.说明 }}</template>
          </dd>
        </dl>
        <div class="evidence-row">
          <div class="evidence">
            <h4>同源已确认记录（{{ replayResult.同源已确认记录.length }}）</h4>
            <ul>
              <li v-for="r in replayResult.同源已确认记录" :key="r.清单ID" class="mono">
                #{{ r.清单ID }} {{ r.窗口版本 }} [{{ r.回执状态 }}]
              </li>
              <li v-if="!replayResult.同源已确认记录.length" class="muted-text">无</li>
            </ul>
          </div>
          <div class="evidence">
            <h4>未核实记录（{{ replayResult.未核实记录.length }}）</h4>
            <ul>
              <li v-for="r in replayResult.未核实记录" :key="r.清单ID" class="mono">
                #{{ r.清单ID }} {{ r.窗口版本 }} [{{ r.回执状态 }}]
              </li>
              <li v-if="!replayResult.未核实记录.length" class="muted-text">无</li>
            </ul>
          </div>
          <div class="evidence evidence-cross">
            <h4>跨源回执不可抵扣（{{ replayResult.跨源回执不可抵扣.length }}）</h4>
            <ul>
              <li v-for="r in replayResult.跨源回执不可抵扣" :key="r.清单ID" class="mono">
                #{{ r.清单ID }} {{ r.源实例 }} {{ r.窗口版本 }}
              </li>
              <li v-if="!replayResult.跨源回执不可抵扣.length" class="muted-text">无</li>
            </ul>
          </div>
        </div>
      </div>
    </article>

    <!-- 恢复清单 -->
    <article class="panel">
      <h3>④ 窗口恢复数据集（恢复清单）</h3>
      <form class="filter-bar" @submit.prevent="reloadManifest">
        <label class="filter-item">
          <span>源实例</span>
          <input v-model="manifestFilters.源实例" data-testid="mf-instance" placeholder="orders-west" />
        </label>
        <label class="filter-item">
          <span>来源表</span>
          <input v-model="manifestFilters.来源表" data-testid="mf-table" />
        </label>
        <label class="filter-item">
          <span>去重键</span>
          <input v-model="manifestFilters.去重键" data-testid="mf-key" />
        </label>
        <label class="filter-item">
          <span>回执状态</span>
          <input v-model="manifestFilters.回执状态" data-testid="mf-receipt" placeholder="已确认 / 未核实" />
        </label>
        <label class="filter-item">
          <span>处置结果</span>
          <input v-model="manifestFilters.处置结果" data-testid="mf-disposition" placeholder="已拦截 / 已放行 / 待重放" />
        </label>
        <button class="btn" type="submit">查询</button>
        <button class="btn ghost" type="button" @click="resetManifestFilters">重置</button>
      </form>
      <div class="table-scroll">
        <table class="data-table">
          <thead>
            <tr>
              <th>ID</th><th>源实例</th><th>来源表</th><th>去重键</th>
              <th>窗口版本（实例/文件@位点）</th><th>事件身份</th><th>回执</th>
              <th>处置结果</th><th>下发判定</th><th>来源文件</th><th>归并</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in manifest" :key="row.id" :data-testid="`manifest-row-${row.id}`">
              <td>{{ row.id }}</td>
              <td>{{ row.源实例 }}</td>
              <td>{{ row.来源表 }}</td>
              <td>{{ row.去重键 }}</td>
              <td class="mono">{{ row.窗口版本 }}</td>
              <td class="mono">{{ row.事件身份 }}</td>
              <td>
                <span :class="['tag', row.回执状态 === '已确认' ? 'tag-ok' : 'tag-warn']">
                  {{ row.回执状态 }}
                </span>
              </td>
              <td>
                <span :class="['tag', dispositionClass(row.处置结果)]">{{ row.处置结果 }}</span>
              </td>
              <td class="reason-cell">{{ row.下发判定 || '—' }}</td>
              <td>{{ row.来源文件.join('、') }}</td>
              <td>{{ row.归并次数 }}</td>
            </tr>
            <tr v-if="!manifest.length">
              <td colspan="11" class="empty-state">暂无恢复清单记录，请先导入交付记录文件</td>
            </tr>
          </tbody>
        </table>
      </div>
    </article>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import {
  fetchImportSessions,
  fetchManifest,
  fetchPending,
  fetchRecoveryStats,
  importDeliveryFile,
  replayCheck,
  resumeImport,
} from '@/api/recovery-service'
import type {
  ImportSession,
  PendingEntry,
  RecoveryManifestRow,
  RecoveryStats,
  ReplayResult,
} from '@/api/recovery-service'

const SCENARIO_KEY = 'ORD-8899'

const stats = ref<Partial<RecoveryStats>>({})
const sessions = ref<ImportSession[]>([])
const pending = ref<PendingEntry[]>([])
const manifest = ref<RecoveryManifestRow[]>([])
const replayResult = ref<ReplayResult | null>(null)
const busy = ref(false)
const message = ref('')
const messageOk = ref(true)

const importForm = reactive({ file_name: '', content: '', batch_size: '' })
const replayForm = reactive({
  source_instance: 'orders-west',
  source_table: 'orders',
  dedup_key: SCENARIO_KEY,
  binlog_file: 'mysql-bin.000201',
  binlog_pos: '8821',
  event_id: '',
})
const manifestFilters = reactive({
  源实例: '',
  来源表: '',
  去重键: '',
  回执状态: '',
  处置结果: '',
})

function flash(text: string, ok = true) {
  message.value = text
  messageOk.value = ok
}

function formatTime(value: string) {
  return value ? value.replace('T', ' ').slice(0, 19) : '—'
}

function sessionStatusClass(status: string) {
  if (status === '已完成') return 'tag-ok'
  if (status === '中断') return 'tag-warn'
  return 'tag-info'
}

function dispositionClass(status: string) {
  if (status === '已拦截') return 'tag-block'
  if (status === '已放行') return 'tag-pass'
  return 'tag-info'
}

async function reloadAll() {
  try {
    const [statsPayload, sessionPayload, pendingPayload, manifestPayload] = await Promise.all([
      fetchRecoveryStats(),
      fetchImportSessions(),
      fetchPending(),
      fetchManifest(manifestFilters),
    ])
    stats.value = statsPayload
    sessions.value = sessionPayload.items
    pending.value = pendingPayload.items
    manifest.value = manifestPayload.items
  } catch (error) {
    flash(error instanceof Error ? error.message : '恢复数据读取失败', false)
  }
}

async function reloadManifest() {
  try {
    const payload = await fetchManifest(manifestFilters)
    manifest.value = payload.items
  } catch (error) {
    flash(error instanceof Error ? error.message : '清单查询失败', false)
  }
}

function resetManifestFilters() {
  Object.keys(manifestFilters).forEach((key) => {
    manifestFilters[key as keyof typeof manifestFilters] = ''
  })
  void reloadManifest()
}

function onPickFile(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return
  importForm.file_name = file.name
  const reader = new FileReader()
  reader.onload = () => {
    importForm.content = String(reader.result ?? '')
  }
  reader.readAsText(file)
}

function fillScenarioSample() {
  const sample = {
    file_name: 'delivery-orders-west-cutover.json',
    source_instance: 'orders-west',
    records: [
      {
        source_table: 'orders',
        dedup_key: SCENARIO_KEY,
        binlog_file: 'mysql-bin.000201',
        binlog_pos: 8821,
        receipt_confirmed: true,
        event_id: 'evt-west-8899',
        operation: 'UPDATE',
      },
      {
        source_table: 'orders',
        dedup_key: 'ORD-8900',
        binlog_file: 'mysql-bin.000201',
        binlog_pos: 9044,
        receipt_confirmed: true,
        event_id: 'evt-west-8900',
        operation: 'INSERT',
      },
      {
        source_table: 'orders',
        dedup_key: 'ORD-8901',
        binlog_file: 'mysql-bin.000202',
        binlog_pos: 310,
        receipt_confirmed: false,
        event_id: 'evt-west-8901',
        operation: 'UPDATE',
      },
    ],
  }
  importForm.file_name = sample.file_name
  importForm.content = JSON.stringify(sample, null, 2)
  flash('已填入仅含 orders-west 已确认日志位置、无 orders-east 回执的样例文件')
}

async function submitImport() {
  if (!importForm.file_name.trim() || !importForm.content.trim()) {
    flash('请填写文件名称并粘贴或选择交付记录内容', false)
    return
  }
  busy.value = true
  try {
    const batchRaw = importForm.batch_size.trim()
    const result = await importDeliveryFile({
      file_name: importForm.file_name.trim(),
      content: importForm.content,
      batch_size: batchRaw === '' ? null : Number(batchRaw),
    })
    if (!result.ok) {
      flash(result.message ?? '导入失败', false)
    } else if (result.idempotent) {
      flash(result.message ?? '该文件已导入，重复导入不生效')
    } else if (result.session?.状态 === '中断') {
      flash(
        `导入中断：已核对 ${result.session.已核对条数}/${result.session.条目总数} 条，剩余条目可在会话中续导`,
        false,
      )
    } else {
      flash(`导入完成：新增 ${result.session?.新增条数 ?? 0} 条，归并 ${result.session?.归并更新条数 ?? 0} 条`)
    }
    await reloadAll()
  } catch (error) {
    flash(error instanceof Error ? error.message : '导入请求失败', false)
  } finally {
    busy.value = false
  }
}

async function submitResume(fingerprint: string, batchSize: number | null) {
  busy.value = true
  try {
    const result = await resumeImport(fingerprint, batchSize)
    flash(result.ok ? `续导完成，会话状态：${result.session?.状态}` : (result.message ?? '续导失败'), result.ok)
    await reloadAll()
  } catch (error) {
    flash(error instanceof Error ? error.message : '续导请求失败', false)
  } finally {
    busy.value = false
  }
}

function fillWestReplay() {
  replayForm.source_instance = 'orders-west'
  replayForm.source_table = 'orders'
  replayForm.dedup_key = SCENARIO_KEY
  replayForm.binlog_file = 'mysql-bin.000201'
  replayForm.binlog_pos = '8821'
  replayForm.event_id = 'evt-west-8899'
}

function fillEastReplay() {
  replayForm.source_instance = 'orders-east'
  replayForm.source_table = 'orders'
  replayForm.dedup_key = SCENARIO_KEY
  replayForm.binlog_file = 'mysql-bin.000307'
  replayForm.binlog_pos = '5520'
  replayForm.event_id = 'evt-east-8899'
}

async function submitReplay() {
  busy.value = true
  try {
    replayResult.value = await replayCheck({
      source_instance: replayForm.source_instance.trim(),
      source_table: replayForm.source_table.trim(),
      dedup_key: replayForm.dedup_key.trim(),
      binlog_file: replayForm.binlog_file.trim(),
      binlog_pos: Number(replayForm.binlog_pos),
      event_id: replayForm.event_id.trim(),
    })
    await reloadAll()
  } catch (error) {
    flash(error instanceof Error ? error.message : '重放核对失败', false)
  } finally {
    busy.value = false
  }
}

onMounted(reloadAll)
</script>

<style scoped>
.recovery-page { display: flex; flex-direction: column; gap: 14px; }
.panel {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 14px 16px;
}
.panel h3 { margin: 0 0 10px; font-size: 15px; }
.hint { color: var(--muted); font-size: 12px; margin: 0 0 12px; }
.import-grid,
.replay-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px 14px;
}
.file-pick input { padding: 6px; }
.json-box {
  width: 100%;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 12px;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px;
  resize: vertical;
}
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
.muted-text { color: var(--muted); font-size: 12px; }
.tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 12px;
  border: 1px solid transparent;
}
.tag-ok { background: #dcfce7; color: #15803d; border-color: #86efac; }
.tag-warn { background: #fef3c7; color: #b45309; border-color: #fcd34d; }
.tag-info { background: #e0f2fe; color: #0369a1; border-color: #7dd3fc; }
.tag-block { background: #fee2e2; color: #b91c1c; border-color: #fca5a5; }
.tag-pass { background: #dcfce7; color: #15803d; border-color: #86efac; }
.pending-box {
  margin-top: 12px;
  padding: 10px 12px;
  background: #fffbeb;
  border: 1px solid #fcd34d;
  border-radius: 6px;
  font-size: 13px;
}
.pending-box ul { margin: 8px 0 0; padding-left: 18px; display: flex; flex-direction: column; gap: 6px; }
.pending-box em { font-style: normal; margin: 0 8px; }
.receipt-yes { color: #15803d; }
.receipt-no { color: #b45309; }
.replay-result {
  margin-top: 14px;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px 14px;
  background: #f8fafc;
}
.decision-head { display: flex; align-items: center; gap: 12px; margin-bottom: 10px; }
.decision {
  display: inline-block;
  padding: 4px 14px;
  border-radius: 6px;
  font-weight: 700;
  color: #fff;
}
.decision-block { background: #dc2626; }
.decision-pass { background: #16a34a; }
.coord-grid {
  display: grid;
  grid-template-columns: 130px 1fr;
  gap: 6px 12px;
  margin: 0 0 12px;
  font-size: 13px;
}
.coord-grid dt { color: var(--muted); }
.coord-grid dd { margin: 0; }
.evidence-row { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
.evidence { border: 1px solid var(--border); border-radius: 6px; padding: 8px 10px; background: #fff; }
.evidence h4 { margin: 0 0 6px; font-size: 12px; color: var(--muted); }
.evidence ul { margin: 0; padding-left: 16px; display: flex; flex-direction: column; gap: 4px; }
.evidence-cross { border-color: #fcd34d; background: #fffbeb; }
.table-scroll { overflow-x: auto; }
.reason-cell { max-width: 260px; font-size: 12px; color: var(--muted); }
.flash { padding: 8px 12px; border-radius: 6px; font-size: 13px; margin: 0; }
.flash-ok { background: #dcfce7; color: #15803d; border: 1px solid #86efac; }
.flash-err { background: #fee2e2; color: #b91c1c; border: 1px solid #fca5a5; }
</style>
