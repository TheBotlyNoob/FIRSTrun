import initImporter, { import_wpilog } from "./firstrun.js";
import initViewer, { WebHandle } from "./re_viewer.js";

const input = document.querySelector("#file-input");
const canvas = document.querySelector("#rerun-canvas");
const channelId = "firstrun-wpilog";

await Promise.all([initImporter(), initViewer("./re_viewer_bg.wasm")]);
const viewer = new WebHandle({ persistState: true });
await viewer.start(canvas);
viewer.open_channel(channelId, "FIRSTrun WPI log");

input.addEventListener("change", async () => {
    const [file] = input.files;
    if (!file) return;

    try {
        const rrd = import_wpilog(new Uint8Array(await file.arrayBuffer()));
        viewer.send_rrd_to_channel(channelId, rrd);
    } catch (error) {
        console.error(error);
        alert(
            "Failed to import WPI log. Please check the console for details."
        );
    }
});
