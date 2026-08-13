/** Oxlint's define helpers are runtime identity functions; keep the vendored plugin self-contained. */
export const definePlugin = <Plugin>(plugin: Plugin): Plugin => plugin;

/** Oxlint's define helpers are runtime identity functions; keep the vendored plugin self-contained. */
export const defineRule = <Rule>(rule: Rule): Rule => rule;
