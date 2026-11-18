export const ROLES={admin:'管理员',operator:'操作员',viewer:'观察员'};
export const STATUSES={queued:'等待执行',running:'试验中',completed:'已完成',failed:'执行失败'};
export const COLUMNS=['sensor_id','recorded_at','device_type','value'];
export const ARCHIVE_STATES={idle:'未归档',migrating:'搬迁中',archived:'已归档'};
export const ARCHIVE_PHASES={copying:'复制中',verifying:'核对中',cleaning:'清理旧副本',completed:'已完成',failed:'已中断'};
