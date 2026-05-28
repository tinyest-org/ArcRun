import { onCleanup, createEffect, on } from 'solid-js';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import type { DagData, DagNode, ColorMap } from './types';
import type { CriticalPath } from './criticalPath';
import { structureChanged } from './dagDiff';
import { colorToHex, disposeObject } from './threeHelpers';
import {
  buildDagSceneObjects,
  buildStatusSceneObjects,
  computeStatusAnimationTargets,
  applyCriticalPathHighlighting,
} from './sceneBuilder';

export interface IsometricDagProps {
  data: DagData | null;
  colorMap: ColorMap;
  criticalPath?: CriticalPath | null;
  onNodeClick: (node: DagNode) => void;
  onBackgroundClick: () => void;
  groupBy?: 'dag' | 'status';
  orderedStatuses?: string[];
  statusShellPriority?: (status: string) => number;
  formatTooltip?: (node: DagNode) => string[];
  onResetCamera?: (fn: () => void) => void;
}

export default function IsometricDag(props: IsometricDagProps) {
  let containerEl!: HTMLDivElement;
  let renderer: THREE.WebGLRenderer | null = null;
  let scene: THREE.Scene | null = null;
  let camera: THREE.OrthographicCamera | null = null;
  let controls: OrbitControls | null = null;
  let animFrameId: number | null = null;
  let tooltipEl: HTMLDivElement | null = null;

  const meshToNode = new Map<THREE.Object3D, DagNode>();
  const nodeToMesh = new Map<string, THREE.Mesh>();
  let currentNodeIds = new Set<string>();
  let currentEdgeIds = new Set<string>();
  let currentStatuses = new Map<string, string>();
  let hoveredMesh: THREE.Mesh | null = null;

  const animTargets = new Map<string, THREE.Vector3>();
  let isAnimating = false;
  let currentGroupBy: 'dag' | 'status' = 'dag';

  const raycaster = new THREE.Raycaster();
  const pointer = new THREE.Vector2();

  const refs = { meshToNode, nodeToMesh };

  function defaultTooltip(node: DagNode): string[] {
    return [node.label, `${node.status} | ${node.group}`];
  }

  function initScene() {
    scene = new THREE.Scene();
    scene.background = new THREE.Color(0x0a0a1a);

    const aspect = containerEl.clientWidth / containerEl.clientHeight;
    const frustum = 20;
    camera = new THREE.OrthographicCamera(
      -frustum * aspect, frustum * aspect, frustum, -frustum, 0.1, 1000,
    );
    camera.position.set(-95, 30, 40);
    camera.lookAt(0, 0, 0);

    renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setSize(containerEl.clientWidth, containerEl.clientHeight);
    renderer.setPixelRatio(window.devicePixelRatio);
    containerEl.appendChild(renderer.domElement);

    controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.1;

    const ambient = new THREE.AmbientLight(0xffffff, 0.6);
    scene.add(ambient);
    const directional = new THREE.DirectionalLight(0xffffff, 0.8);
    directional.position.set(10, 20, 10);
    scene.add(directional);

    const grid = new THREE.GridHelper(50, 50, 0x333344, 0x222233);
    scene.add(grid);

    tooltipEl = document.createElement('div');
    tooltipEl.style.cssText =
      'position:fixed;pointer-events:none;background:rgba(0,0,0,0.9);color:#fff;padding:8px 12px;border-radius:6px;font-size:12px;font-family:monospace;display:none;z-index:1000;white-space:pre;line-height:1.5;border:1px solid rgba(255,255,255,0.1);';
    containerEl.appendChild(tooltipEl);

    renderer.domElement.addEventListener('pointermove', onPointerMove);
    renderer.domElement.addEventListener('click', onClick);
    window.addEventListener('resize', onResize);

    function animate() {
      animFrameId = requestAnimationFrame(animate);

      if (isAnimating) {
        let allDone = true;
        for (const [nodeId, target] of animTargets) {
          const mesh = nodeToMesh.get(nodeId);
          if (!mesh) continue;
          if (mesh.position.distanceTo(target) > 0.01) {
            mesh.position.lerp(target, 0.08);
            allDone = false;
          } else {
            mesh.position.copy(target);
          }
        }
        if (allDone) {
          isAnimating = false;
          animTargets.clear();
        }
      }

      controls!.update();
      renderer!.render(scene!, camera!);
    }
    animate();

    props.onResetCamera?.(() => {
      if (!camera || !controls) return;
      camera.position.set(-95, 30, 40);
      camera.lookAt(0, 0, 0);
      controls.target.set(0, 0, 0);
      controls.update();
    });
  }

  function onResize() {
    if (!renderer || !camera) return;
    const w = containerEl.clientWidth;
    const h = containerEl.clientHeight;
    const aspect = w / h;
    const frustum = camera.top;
    camera.left = -frustum * aspect;
    camera.right = frustum * aspect;
    camera.updateProjectionMatrix();
    renderer.setSize(w, h);
  }

  function onPointerMove(event: PointerEvent) {
    if (!renderer || !camera || !scene) return;
    const rect = renderer.domElement.getBoundingClientRect();
    pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;

    raycaster.setFromCamera(pointer, camera);
    const nodeMeshes = Array.from(meshToNode.keys()) as THREE.Mesh[];
    const intersects = raycaster.intersectObjects(nodeMeshes);

    if (hoveredMesh) {
      const prevNode = meshToNode.get(hoveredMesh);
      const isCritical = prevNode && props.criticalPath?.nodeIds.has(prevNode.id);
      (hoveredMesh.material as THREE.MeshLambertMaterial).emissive.setHex(
        isCritical ? 0x665500 : 0x000000,
      );
      hoveredMesh = null;
    }
    if (tooltipEl) tooltipEl.style.display = 'none';

    if (intersects.length > 0) {
      const mesh = intersects[0].object as THREE.Mesh;
      const node = meshToNode.get(mesh);
      if (node) {
        hoveredMesh = mesh;
        (mesh.material as THREE.MeshLambertMaterial).emissive.setHex(0x222222);
        renderer.domElement.style.cursor = 'pointer';

        if (tooltipEl) {
          const formatter = props.formatTooltip ?? defaultTooltip;
          const lines = formatter(node);
          tooltipEl.textContent = lines.join('\n');
          tooltipEl.style.display = 'block';
          tooltipEl.style.left = `${event.clientX + 12}px`;
          tooltipEl.style.top = `${event.clientY + 12}px`;
        }
      }
    } else {
      renderer.domElement.style.cursor = 'grab';
    }
  }

  function onClick(event: MouseEvent) {
    if (!renderer || !camera || !scene) return;
    const rect = renderer.domElement.getBoundingClientRect();
    pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;

    raycaster.setFromCamera(pointer, camera);
    const nodeMeshes = Array.from(meshToNode.keys()) as THREE.Mesh[];
    const intersects = raycaster.intersectObjects(nodeMeshes);

    if (intersects.length > 0) {
      const node = meshToNode.get(intersects[0].object);
      if (node) props.onNodeClick(node);
    } else {
      props.onBackgroundClick();
    }
  }

  function clearScene() {
    if (!scene) return;
    isAnimating = false;
    animTargets.clear();
    const toRemove: THREE.Object3D[] = [];
    scene.traverse((obj) => {
      if (obj.userData.isTask || obj.userData.isGroup || obj.userData.isLink || obj.userData.isLabel) {
        toRemove.push(obj);
      }
    });
    for (const obj of toRemove) {
      disposeObject(obj);
      scene!.remove(obj);
    }
    meshToNode.clear();
    nodeToMesh.clear();
  }

  function clearStructuralElements() {
    if (!scene) return;
    const toRemove: THREE.Object3D[] = [];
    scene.traverse((obj) => {
      if (obj.userData.isGroup || obj.userData.isLink || obj.userData.isLabel) {
        toRemove.push(obj);
      }
    });
    for (const obj of toRemove) {
      disposeObject(obj);
      scene!.remove(obj);
    }
  }

  function fitCamera(groupCount: number, maxStep: number, cellSizeX: number, cellSizeZ: number) {
    if (!camera) return;
    const extentX = groupCount * cellSizeX;
    const extentZ = (maxStep + 1) * cellSizeZ;
    const extent = Math.max(extentX, extentZ, 10) * 0.7;
    const aspect = containerEl.clientWidth / containerEl.clientHeight;
    camera.top = extent;
    camera.bottom = -extent;
    camera.left = -extent * aspect;
    camera.right = extent * aspect;
    camera.updateProjectionMatrix();
  }

  function fitCameraExtent(extentX: number, extentZ: number) {
    if (!camera) return;
    const extent = Math.max(extentX, extentZ, 10) * 0.7;
    const aspect = containerEl.clientWidth / containerEl.clientHeight;
    camera.top = extent;
    camera.bottom = -extent;
    camera.left = -extent * aspect;
    camera.right = extent * aspect;
    camera.updateProjectionMatrix();
  }

  function buildScene(data: DagData) {
    if (!scene) return;
    clearScene();
    const result = buildDagSceneObjects(scene, data, refs, props.colorMap, props.statusShellPriority);
    fitCamera(result.groupCount, result.maxStep, result.cellSizeX, result.cellSizeZ);
    currentNodeIds = new Set(data.nodes.map((n) => n.id));
    currentEdgeIds = new Set(data.edges.map((e) => `${e.source}-${e.target}`));
    currentStatuses = new Map(data.nodes.map((n) => [n.id, n.status]));
  }

  function updateColorsInPlace(nodes: DagNode[]) {
    for (const node of nodes) {
      const mesh = nodeToMesh.get(node.id);
      if (!mesh) continue;
      const color = props.colorMap[node.status] ?? '#666666';
      (mesh.material as THREE.MeshLambertMaterial).color.setHex(colorToHex(color));
      meshToNode.set(mesh, node);
    }
    currentStatuses = new Map(nodes.map((n) => [n.id, n.status]));
  }

  function nodeSetChanged(nodes: DagNode[]): boolean {
    if (nodes.length !== currentNodeIds.size) return true;
    for (const n of nodes) {
      if (!currentNodeIds.has(n.id)) return true;
    }
    return false;
  }

  function buildStatusScene(data: DagData) {
    if (!scene) return;
    clearScene();
    const result = buildStatusSceneObjects(scene, data, refs, props.colorMap, props.orderedStatuses);
    fitCameraExtent(result.extentX, result.extentZ);
    currentNodeIds = new Set(data.nodes.map((n) => n.id));
    currentEdgeIds = new Set(data.edges.map((e) => `${e.source}-${e.target}`));
    currentStatuses = new Map(data.nodes.map((n) => [n.id, n.status]));
  }

  function animateToStatusLayout(data: DagData) {
    if (!scene) return;

    for (const node of data.nodes) {
      const mesh = nodeToMesh.get(node.id);
      if (mesh) {
        const color = props.colorMap[node.status] ?? '#666666';
        (mesh.material as THREE.MeshLambertMaterial).color.setHex(colorToHex(color));
        meshToNode.set(mesh, node);
      }
    }

    clearStructuralElements();
    const result = computeStatusAnimationTargets(scene, data, refs, props.colorMap, props.orderedStatuses);

    animTargets.clear();
    for (const [nodeId, pos] of result.targets) {
      animTargets.set(nodeId, pos);
    }
    isAnimating = true;

    fitCameraExtent(result.extentX, result.extentZ);
    currentStatuses = new Map(data.nodes.map((n) => [n.id, n.status]));
  }

  createEffect(
    on(
      () => props.criticalPath,
      (cp) => {
        if (scene) applyCriticalPathHighlighting(scene, nodeToMesh, cp);
      },
    ),
  );

  createEffect(
    on(
      [() => props.data, () => props.groupBy ?? 'dag'] as const,
      ([data, groupBy]) => {
        if (!data || data.nodes.length === 0) {
          clearScene();
          currentNodeIds.clear();
          currentEdgeIds.clear();
          currentStatuses.clear();
          return;
        }

        if (!renderer) {
          initScene();
        }

        const groupByChanged = groupBy !== currentGroupBy;
        currentGroupBy = groupBy;

        if (groupBy === 'status') {
          if (groupByChanged || currentNodeIds.size === 0 || nodeSetChanged(data.nodes)) {
            buildStatusScene(data);
          } else {
            animateToStatusLayout(data);
          }
        } else {
          if (!groupByChanged && currentNodeIds.size > 0 && !structureChanged(data.nodes, data.edges, currentNodeIds, currentEdgeIds)) {
            updateColorsInPlace(data.nodes);
          } else {
            buildScene(data);
          }
        }
      },
    ),
  );

  onCleanup(() => {
    if (animFrameId !== null) cancelAnimationFrame(animFrameId);
    if (controls) controls.dispose();
    if (renderer) {
      renderer.domElement.removeEventListener('pointermove', onPointerMove);
      renderer.domElement.removeEventListener('click', onClick);
      renderer.dispose();
      if (renderer.domElement.parentNode) {
        renderer.domElement.parentNode.removeChild(renderer.domElement);
      }
    }
    if (tooltipEl && tooltipEl.parentNode) {
      tooltipEl.parentNode.removeChild(tooltipEl);
    }
    window.removeEventListener('resize', onResize);
    isAnimating = false;
    animTargets.clear();
    clearScene();
    scene = null;
    camera = null;
    renderer = null;
    controls = null;
  });

  return (
    <div
      ref={containerEl}
      class="flex-1"
      style={{ background: 'transparent' }}
    />
  );
}
