/** 去重窗口恢复：从历史交付记录文件导入恢复数据集的接口封装。 */
import { request } from './client'

export type RecoveryStats = {
  恢复清单总数: number
  已确认回执数: number
  待核实记录数: number
  清单当前已拦截数: number
  清单当前已放行数: number
  待重放数: number
  累计拦截重放数: number
  累计放行重放数: number
  导入会话数: number
  中断会话数: number
  暂存待核对条数: number
}

export type RecoveryManifestRow = {
  id: number
  源实例: string
  来源表: string
  去重键: string
  日志文件: string
  日志位点: number
  事件身份: string
  窗口版本: string
  事件ID: string
  操作类型: string
  事件时间: string
  回执状态: string
  核实状态: string
  处置结果: string
  下发判定: string
  处置备注: string
  处置时间: string | null
  最近回执位点: number | null
  来源文件: string[]
  导入批次: number
  归并次数: number
  导入时间: string
}

export type ImportSession = {
  id: number
  文件名称: string
  文件指纹: string
  状态: string
  条目总数: number
  已核对条数: number
  无效条数: number
  新增条数: number
  归并更新条数: number
  重复条数: number
  无效明细: { 序号: number; 原因: string; 原始: unknown }[]
  创建时间: string
  最近更新时间: string
}

export type PendingEntry = {
  文件指纹: string
  序号: number
  条目: {
    source_instance: string
    source_table: string
    dedup_key: string
    binlog_file: string
    binlog_pos: number
    receipt_confirmed: boolean
  }
}

export type ReplayResult = {
  ok: boolean
  message?: string
  事件身份: string
  下发决定: string
  决定原因: string
  源坐标核对: Record<string, string | number>
  窗口版本比对: {
    当前窗口版本: string
    历史窗口版本: string | null
    比对结论: string
  }
  同源已确认记录: { 清单ID: number; 窗口版本: string; 回执状态: string }[]
  未核实记录: { 清单ID: number; 窗口版本: string; 回执状态: string }[]
  跨源回执不可抵扣: { 清单ID: number; 源实例: string; 窗口版本: string }[]
  命中清单ID: number | null
  回写结果: { 已回写: boolean; 清单ID?: number; 处置结果?: string; 说明?: string }
}

const BASE = '/api/dedup_window/recovery'

async function readJson<T>(response: Response): Promise<T> {
  if (!response.ok) throw new Error(`接口返回 ${response.status}`)
  return (await response.json()) as T
}

export async function fetchRecoveryStats(): Promise<RecoveryStats> {
  return readJson(await request(`${BASE}/stats`))
}

export async function fetchManifest(filters: Record<string, string> = {}): Promise<{
  items: RecoveryManifestRow[]
  total: number
}> {
  const query = new URLSearchParams(
    Object.entries(filters).filter(([, value]) => value),
  ).toString()
  return readJson(await request(`${BASE}/manifest?${query}`))
}

export async function fetchImportSessions(): Promise<{ items: ImportSession[]; total: number }> {
  return readJson(await request(`${BASE}/imports`))
}

export async function fetchPending(fingerprint = ''): Promise<{
  items: PendingEntry[]
  total: number
}> {
  const suffix = fingerprint ? `?fingerprint=${encodeURIComponent(fingerprint)}` : ''
  return readJson(await request(`${BASE}/pending${suffix}`))
}

export async function importDeliveryFile(input: {
  file_name: string
  content: string
  batch_size?: number | null
}): Promise<{ ok: boolean; idempotent?: boolean; message?: string; session?: ImportSession }> {
  return readJson(
    await request(`${BASE}/imports`, {
      method: 'POST',
      body: JSON.stringify(input),
    }),
  )
}

export async function resumeImport(
  fingerprint: string,
  batchSize?: number | null,
): Promise<{ ok: boolean; idempotent?: boolean; message?: string; session?: ImportSession }> {
  return readJson(
    await request(`${BASE}/imports/resume`, {
      method: 'POST',
      body: JSON.stringify({ fingerprint, batch_size: batchSize ?? null }),
    }),
  )
}

export async function replayCheck(input: {
  source_instance: string
  source_table: string
  dedup_key: string
  binlog_file: string
  binlog_pos: number
  event_id?: string
  operation?: string
}): Promise<ReplayResult> {
  return readJson(
    await request(`${BASE}/replay-check`, {
      method: 'POST',
      body: JSON.stringify(input),
    }),
  )
}
