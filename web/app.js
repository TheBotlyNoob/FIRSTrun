import initImporter, { import_wpilog } from "./firstrun.js";
import initViewer, { WebHandle } from "./re_viewer.js";

const input = document.querySelector("#file-input");
const canvas = document.querySelector("#rerun-canvas");
const status = document.querySelector("#status");
const channelId = "firstrun-wpilog";

await Promise.all([initImporter(), initViewer()]);
const viewer = new WebHandle({ persistState: true });
await viewer.start(canvas);
viewer.open_channel(channelId, "FIRSTrun WPI log");

input.addEventListener("change", async () => {
    const [file] = input.files;
    if (!file) return;

    status.textContent = `Loading ${file.name}...`;
    try {
        const rrd = import_wpilog(new Uint8Array(await file.arrayBuffer()));
        viewer.send_rrd_to_channel(channelId, rrd);
        status.textContent = `${file.name} loaded`;
    } catch (error) {
        status.textContent =
            error instanceof Error ? error.message : String(error);
    }
});
