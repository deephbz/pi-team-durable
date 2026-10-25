/**
 * Unstable test support for hosts that exercise authority internals.
 * It names only the internals HyperCarrier's AmPi tests import. Production code must use `./authority`.
 * Names and behavior can change in any release without a major version.
 */
export { Deliver } from './authority/delivery.ts';
export { createAuthorityTools,type CoreApi } from './authority/tools.ts';
export { addAlert,addObligation,flushAuthority,graphCommand,obligation,setObligationState,taskRecord,type Authority } from './authority/store.ts';
export type { Alert,Obligation } from './authority/state.ts';
export { ObservationHeadDoc,PageDoc,PageBodyDoc,reserve,startObservationEpoch } from './authority/observation.ts';
export { advanceAcknowledged,page,retireBody,savePage } from './authority/observation-store.ts';
