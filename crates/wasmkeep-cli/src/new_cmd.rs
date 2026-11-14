//! `wasmkeep new`: writes a project that builds and runs the first time
//! (`wasmkeep dev`), from templates compiled into the binary: no network.
//!
//! A project is a class `counter` (one durable counter per actor) and,
//! with `--front`, a front over it, in Rust or TypeScript, with its
//! `wasmkeep.toml` (ADR 0026), a README, a `.gitignore` and an
//! `.env.local.example`. The SDK is pinned to this binary's own version:
//! the version the release publishes the SDKs at, so a released `wasmkeep`
//! writes a project its registries can build. A `wasmkeep` built from a
//! checkout writes its version too; until that version is on the
//! registries, build such a project against the checkout's SDK
//! (docs/tutorials/first-actor.md says how).

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use clap::Args;

#[derive(Debug, Args)]
pub(crate) struct NewArgs {
    /// The directory to write the project in: created if it does not exist,
    /// refused if it is not empty.
    dir: PathBuf,
    /// The language of the class and of the front.
    #[arg(long, value_enum)]
    lang: Lang,
    /// Add a front: a page and an API over the class, which then stays
    /// `internal` (only the front reaches it). Without one, the class is
    /// `open`, at /actor/counter/<name>.
    #[arg(long)]
    front: bool,
    /// TENANT/APP for `[app]` (each `[a-z][a-z0-9-]{0,63}`). Default:
    /// tenant `my-team`, and the directory's name as the app's.
    #[arg(long, value_name = "TENANT/APP")]
    app: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Lang {
    Rust,
    #[value(name = "typescript", alias = "ts")]
    TypeScript,
}

/// The version the SDKs are pinned to: the workspace version, which a
/// release publishes every SDK at (`wasmkeep --version` adds the commit to
/// a build that is not a release's; this never does).
const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The tenant when `--app` names none.
const DEFAULT_TENANT: &str = "my-team";

const RUST_COUNTER_CARGO: &str = include_str!("../templates/rust/counter.Cargo.toml.txt");
const RUST_COUNTER_LIB: &str = include_str!("../templates/rust/counter.lib.rs");
const RUST_FRONT_CARGO: &str = include_str!("../templates/rust/front.Cargo.toml.txt");
const RUST_FRONT_LIB: &str = include_str!("../templates/rust/front.lib.rs");
const TS_PACKAGE: &str = include_str!("../templates/typescript/package.json.txt");
const TS_PACKAGE_FRONT: &str = include_str!("../templates/typescript/package-front.json.txt");
const TS_CONFIG: &str = include_str!("../templates/typescript/tsconfig.json.txt");
const TS_COUNTER: &str = include_str!("../templates/typescript/counter.ts");
const TS_FRONT: &str = include_str!("../templates/typescript/front.ts");
const GITIGNORE: &str = include_str!("../templates/common/gitignore.txt");
const ENV_EXAMPLE: &str = include_str!("../templates/common/env.local.example.txt");
const README: &str = include_str!("../templates/common/README.md.txt");

/// What to write: the project's files, by path relative to its directory.
#[derive(Debug)]
struct Plan {
    tenant: String,
    app: String,
    lang: Lang,
    front: bool,
}

pub(crate) fn run(args: &NewArgs) -> anyhow::Result<()> {
    let (tenant, app) = match &args.app {
        Some(given) => {
            let (tenant, app) = crate::parse_app(given).map_err(anyhow::Error::msg)?;
            (tenant, app)
        }
        None => (DEFAULT_TENANT.to_owned(), app_from_dir(&args.dir)?),
    };
    for (what, name) in [("tenant", &tenant), ("app", &app)] {
        anyhow::ensure!(
            wasmkeep_runtime::is_identifier(name),
            "the {what} {name:?} must match [a-z][a-z0-9-]{{0,63}} (lowercase letters, digits \
             and '-', starting with a letter): pass --app TENANT/APP"
        );
    }
    refuse_unless_empty(&args.dir)?;
    let plan = Plan {
        tenant,
        app,
        lang: args.lang,
        front: args.front,
    };
    let files = files(&plan);
    std::fs::create_dir_all(&args.dir)
        .with_context(|| format!("cannot create {}", args.dir.display()))?;
    for (path, text) in &files {
        let path = args.dir.join(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("cannot create {}", parent.display()))?;
        }
        // Never over a file: the directory was empty a moment ago.
        use std::io::Write as _;
        std::fs::File::options()
            .write(true)
            .create_new(true)
            .open(&path)
            .and_then(|mut f| f.write_all(text.as_bytes()))
            .with_context(|| format!("cannot write {}", path.display()))?;
    }
    print!("{}", next_steps(&plan, &args.dir));
    Ok(())
}

/// The app's name from the directory's: its last component, lowercased.
fn app_from_dir(dir: &Path) -> anyhow::Result<String> {
    let absolute = std::path::absolute(dir).unwrap_or_else(|_| dir.to_owned());
    let name = absolute
        .file_name()
        .and_then(|n| n.to_str())
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    anyhow::ensure!(
        wasmkeep_runtime::is_identifier(&name),
        "the directory's name {name:?} is not an app name ([a-z][a-z0-9-]{{0,63}}): \
         pass --app TENANT/APP"
    );
    Ok(name)
}

fn refuse_unless_empty(dir: &Path) -> anyhow::Result<()> {
    match std::fs::read_dir(dir) {
        Ok(mut entries) => {
            anyhow::ensure!(
                entries.next().is_none(),
                "{} is not empty: wasmkeep new writes into an empty or new directory only",
                dir.display()
            );
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("cannot read {}", dir.display())),
    }
}

/// Every file of the project, its placeholders filled.
fn files(plan: &Plan) -> Vec<(&'static str, String)> {
    let fill = |text: &str| {
        text.replace("{{version}}", SDK_VERSION)
            .replace("{{tenant}}", &plan.tenant)
            .replace("{{app}}", &plan.app)
    };
    let mut files: Vec<(&'static str, String)> = Vec::new();
    match plan.lang {
        Lang::Rust => {
            files.push(("counter/Cargo.toml", fill(RUST_COUNTER_CARGO)));
            files.push(("counter/src/lib.rs", fill(RUST_COUNTER_LIB)));
            if plan.front {
                files.push(("front/Cargo.toml", fill(RUST_FRONT_CARGO)));
                files.push(("front/src/lib.rs", fill(RUST_FRONT_LIB)));
            }
        }
        Lang::TypeScript => {
            let package = if plan.front {
                TS_PACKAGE_FRONT
            } else {
                TS_PACKAGE
            };
            files.push(("package.json", fill(package)));
            files.push(("tsconfig.json", fill(TS_CONFIG)));
            files.push(("src/counter.ts", fill(TS_COUNTER)));
            if plan.front {
                files.push(("src/front.ts", fill(TS_FRONT)));
            }
        }
    }
    files.push(("wasmkeep.toml", project_file(plan)));
    files.push(("README.md", readme(plan)));
    files.push((".gitignore", GITIGNORE.to_owned()));
    files.push((".env.local.example", ENV_EXAMPLE.to_owned()));
    files
}

/// A part's component, build command and watched paths.
struct Part {
    component: &'static str,
    build: &'static str,
    watch: &'static str,
}

fn part(lang: Lang, front: bool) -> Part {
    match (lang, front) {
        (Lang::Rust, false) => Part {
            component: "counter/target/wasm32-wasip2/release/counter.wasm",
            build: "cd counter && cargo build --release --target wasm32-wasip2",
            watch: r#"["counter/src", "counter/Cargo.toml"]"#,
        },
        (Lang::Rust, true) => Part {
            component: "front/target/wasm32-wasip2/release/front.wasm",
            build: "cd front && cargo build --release --target wasm32-wasip2",
            watch: r#"["front/src", "front/Cargo.toml"]"#,
        },
        (Lang::TypeScript, false) => Part {
            component: "counter.wasm",
            build: "npm run build:counter",
            watch: r#"["src/counter.ts"]"#,
        },
        (Lang::TypeScript, true) => Part {
            component: "front.wasm",
            build: "npm run build:front",
            watch: r#"["src/front.ts"]"#,
        },
    }
}

fn project_file(plan: &Plan) -> String {
    let mut out = format!(
        "# The project file (https://docs.wasmkeep.dev/project-file.html):\n\
         # `wasmkeep dev` runs this app locally, `wasmkeep deploy --bucket ...`\n\
         # deploys exactly it.\n\n\
         [app]\n\
         # Nodes find the app by these two: choose them before the first deploy.\n\
         tenant = \"{}\"\nname = \"{}\"\n",
        plan.tenant, plan.app
    );
    if plan.front {
        let f = part(plan.lang, true);
        out.push_str(&format!(
            "\n# Every request outside /actor/ goes to the front.\n[front]\n\
             component = \"{}\"\nbuild = \"{}\"\nwatch = {}\n",
            f.component, f.build, f.watch
        ));
    }
    let c = part(plan.lang, false);
    let access = if plan.front {
        "# access = \"internal\" is the default: only the app's front reaches it.\n"
    } else {
        "# Anyone may reach /actor/counter/<name>. With a front, leave this out:\n\
         # the class is then internal, reached by the front only.\n\
         access = \"open\"\n"
    };
    out.push_str(&format!(
        "\n[classes.counter]\ncomponent = \"{}\"\n{access}build = \"{}\"\nwatch = {}\n",
        c.component, c.build, c.watch
    ));
    out.push_str("\n[dev]\nport = 8080\n");
    out
}

fn readme(plan: &Plan) -> String {
    let (language, needs, install) = match plan.lang {
        Lang::Rust => (
            "Rust",
            "`wasmkeep`, and [rustup](https://rustup.rs) with the WebAssembly target",
            "rustup target add wasm32-wasip2",
        ),
        Lang::TypeScript => (
            "TypeScript",
            "`wasmkeep`, and Node.js 22 or later with npm",
            "npm install",
        ),
    };
    let mut layout = String::new();
    let row = |path: &str, what: &str| format!("| `{path}` | {what} |\n");
    match plan.lang {
        Lang::Rust => {
            layout.push_str(&row(
                "counter/",
                "class `counter`: one durable counter per actor, in its own SQLite database",
            ));
            if plan.front {
                layout.push_str(&row(
                    "front/",
                    "the front: a page and an API over the class",
                ));
            }
        }
        Lang::TypeScript => {
            layout.push_str(&row(
                "src/counter.ts",
                "class `counter`: one durable counter per actor, in its own SQLite database",
            ));
            if plan.front {
                layout.push_str(&row(
                    "src/front.ts",
                    "the front: a page and an API over the class",
                ));
            }
        }
    }
    let try_it = if plan.front {
        "curl localhost:8080/                           # the page (or open it in a browser)\n\
         curl -X POST localhost:8080/api/counter/alice  # 1\n\
         curl -X POST localhost:8080/api/counter/alice  # 2\n\
         curl localhost:8080/api/counter/bob            # 0, another actor\n\
         curl -i localhost:8080/actor/counter/alice     # 403: the class is internal"
    } else {
        "curl -X POST localhost:8080/actor/counter/alice  # 1\n\
         curl -X POST localhost:8080/actor/counter/alice  # 2\n\
         curl localhost:8080/actor/counter/bob            # 0, another actor"
    };
    README
        .replace("{{layout}}", &layout)
        .replace("{{language}}", language)
        .replace("{{needs}}", needs)
        .replace("{{install}}", install)
        .replace("{{try}}", try_it)
        .replace("{{version}}", SDK_VERSION)
        .replace("{{app}}", &plan.app)
}

fn next_steps(plan: &Plan, dir: &Path) -> String {
    let what = match (plan.lang, plan.front) {
        (Lang::Rust, false) => "Rust: class counter",
        (Lang::Rust, true) => "Rust: class counter and a front",
        (Lang::TypeScript, false) => "TypeScript: class counter",
        (Lang::TypeScript, true) => "TypeScript: class counter and a front",
    };
    let install = match plan.lang {
        Lang::Rust => "rustup target add wasm32-wasip2",
        Lang::TypeScript => "npm install",
    };
    format!(
        "wrote {tenant}/{app} in {dir} ({what})\n\nnext:\n  cd {dir}\n  {install}\n  \
         wasmkeep dev\n\nthen, to deploy it (see README.md):\n  \
         wasmkeep deploy --bucket s3://BUCKET/PREFIX\n",
        tenant = plan.tenant,
        app = plan.app,
        dir = dir.display(),
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)] // test code

    use super::*;

    fn every_plan() -> Vec<Plan> {
        let mut plans = Vec::new();
        for lang in [Lang::Rust, Lang::TypeScript] {
            for front in [false, true] {
                plans.push(Plan {
                    tenant: "acme".into(),
                    app: "hits".into(),
                    lang,
                    front,
                });
            }
        }
        plans
    }

    #[test]
    fn every_project_file_reads_as_wasmkeep_dev_reads_it() {
        for plan in every_plan() {
            let text = project_file(&plan);
            let project = crate::project::parse(&text, Path::new("/p/wasmkeep.toml"))
                .unwrap_or_else(|e| panic!("{plan:?}: {e}\n{text}"));
            assert_eq!(
                (project.tenant.as_str(), project.name.as_str()),
                ("acme", "hits")
            );
            assert_eq!(project.front.is_some(), plan.front, "{text}");
            let counter = &project.classes["counter"];
            let access = if plan.front { "internal" } else { "open" };
            assert_eq!(counter.access, access, "{text}");
            assert!(counter.item.build.is_some() && !counter.item.watch.is_empty());
            // Nothing the first run would wait for: a required name
            // unset answers 503.
            assert!(counter.item.requires.is_empty());
            assert!(project.front.iter().all(|f| f.requires.is_empty()));
            assert_eq!(project.dev.port, Some(8080));
        }
    }

    #[test]
    fn no_placeholder_is_left_and_the_sdk_is_this_version() {
        for plan in every_plan() {
            let files = files(&plan);
            for (path, text) in &files {
                assert!(!text.contains("{{"), "{plan:?} {path}: {text}");
            }
            let pinned = files
                .iter()
                .filter(|(p, _)| p.ends_with("Cargo.toml") || *p == "package.json")
                .all(|(_, t)| t.contains(&format!("\"{}\"", SDK_VERSION)));
            assert!(pinned, "{plan:?}");
            let front_file = |p: &str| p.starts_with("front/") || p == "src/front.ts";
            assert_eq!(files.iter().any(|(p, _)| front_file(p)), plan.front);
        }
        // A version as the registries have it: no build metadata.
        assert!(
            SDK_VERSION.bytes().all(|b| b.is_ascii_digit() || b == b'.'),
            "{}",
            SDK_VERSION
        );
    }

    /// The templates name the version by its placeholder only, so a
    /// version bump has nothing to change here (scripts/check-versions.py
    /// lists no template).
    #[test]
    fn the_templates_carry_no_version_of_their_own() {
        for template in [
            RUST_COUNTER_CARGO,
            RUST_FRONT_CARGO,
            TS_PACKAGE,
            TS_PACKAGE_FRONT,
        ] {
            assert!(template.contains("\"{{version}}\""), "{template}");
            assert!(!template.contains(SDK_VERSION), "{template}");
        }
    }

    #[test]
    fn the_app_is_named_after_the_directory() {
        assert_eq!(app_from_dir(Path::new("/x/Hits")).unwrap(), "hits");
        assert_eq!(app_from_dir(Path::new("shop/")).unwrap(), "shop");
        assert!(app_from_dir(Path::new("/x/my_app")).is_err());
        assert!(app_from_dir(Path::new("/x/9lives")).is_err());
    }
}
