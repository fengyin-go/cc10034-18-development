import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import RecoveryView from '@/views/RecoveryView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/dedup_window/recovery', name: 'dedup-recovery', component: RecoveryView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
